package com.gojek.mc2pinot.core;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.core.partition.PartitionSpec;
import com.gojek.mc2pinot.core.partition.SegmentAssigner;
import com.gojek.mc2pinot.core.reader.ConcatenatingRecordReader;
import com.gojek.mc2pinot.core.reader.EmptyRecordReader;
import com.gojek.mc2pinot.core.splitter.AvroFileSplitter;
import com.gojek.mc2pinot.core.splitter.FileSplitter;
import com.gojek.mc2pinot.core.splitter.JsonFileSplitter;
import com.gojek.mc2pinot.core.splitter.ParquetFileSplitter;
import com.gojek.mc2pinot.io.Reader;
import com.gojek.mc2pinot.io.Writer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class PinotSegmenter {

    private static final Logger LOG = Logger.getLogger(PinotSegmenter.class.getName());

    private final Reader reader;
    private final Writer writer;
    private final String segmentKey;
    private final String inputFormat;
    private final Schema schema;
    private final TableConfig tableConfig;
    private final PartitionFunction partitionFunction;
    /**
     * Max concurrent file splits.  Each concurrent split holds {@code numPartitions}
     * Parquet/Avro/JSON writers in memory simultaneously, so keeping this low is
     * the primary lever for controlling peak RAM in the split phase.
     */
    private final int splitParallelism;

    /**
     * Max concurrent segment builds.  Each Pinot
     * {@link SegmentIndexCreationDriverImpl} build loads its full partition into
     * memory, so this controls peak RAM in the build phase.
     */
    private final int buildParallelism;

    /**
     * Configured number of physical segments to emit. {@code 0} (the default) means
     * "fall back to the table's partition count", preserving the original behaviour.
     * Set via {@link #setSegmentCount(int)}.
     */
    private int segmentCount = 0;

    /** Convenience constructor – parallelism defaults to the number of available CPU cores. */
    public PinotSegmenter(Reader reader, Writer writer, String segmentKey,
                          String inputFormat, Schema schema, TableConfig tableConfig,
                          PartitionFunction partitionFunction) {
        this(reader, writer, segmentKey, inputFormat, schema, tableConfig, partitionFunction,
                Runtime.getRuntime().availableProcessors());
    }

    /**
     * Single-parallelism constructor (backward-compatible).
     * Split concurrency is automatically capped at {@code min(parallelism, 2)} because
     * each concurrent split keeps {@code numPartitions} writer buffers alive in RAM.
     * Build concurrency uses the full {@code parallelism} value.
     *
     * @param parallelism desired level of concurrency; pass {@code 1} to disable parallelism
     */
    public PinotSegmenter(Reader reader, Writer writer, String segmentKey,
                          String inputFormat, Schema schema, TableConfig tableConfig,
                          PartitionFunction partitionFunction, int parallelism) {
        this(reader, writer, segmentKey, inputFormat, schema, tableConfig, partitionFunction,
                Math.min(parallelism, 2), parallelism);
    }

    /**
     * Full constructor with independent control over split and build concurrency.
     *
     * @param splitParallelism  max concurrent input-file splits (keep small to limit
     *                          the number of open partition writers in RAM)
     * @param buildParallelism  max concurrent segment builds
     */
    public PinotSegmenter(Reader reader, Writer writer, String segmentKey,
                          String inputFormat, Schema schema, TableConfig tableConfig,
                          PartitionFunction partitionFunction,
                          int splitParallelism, int buildParallelism) {
        if (splitParallelism < 1) throw new IllegalArgumentException("splitParallelism must be >= 1");
        if (buildParallelism < 1) throw new IllegalArgumentException("buildParallelism must be >= 1");
        this.reader = reader;
        this.writer = writer;
        this.segmentKey = segmentKey;
        this.inputFormat = inputFormat.toLowerCase();
        this.schema = schema;
        this.tableConfig = tableConfig;
        this.partitionFunction = partitionFunction;
        this.splitParallelism = splitParallelism;
        this.buildParallelism = buildParallelism;
    }

    /**
     * Sets the desired number of physical segments (S). Pass {@code <= 0} to fall back to the
     * table's partition count (P). Returns {@code this} for fluent chaining.
     */
    public PinotSegmenter setSegmentCount(int segmentCount) {
        this.segmentCount = segmentCount;
        return this;
    }

    public GenerationResult generateSegment() throws Exception {
        String tableName = tableConfig.getTableName();
        PartitionSpec partitionSpec = extractPartitionSpec();

        LOG.info("transient(core): start generating " + partitionSpec.count()
                + " segments (partitionCount=" + partitionSpec.partitionCount()
                + ") with splitParallelism=" + splitParallelism
                + " buildParallelism=" + buildParallelism + "...");

        // One assigner shared by all concurrent splitters so the S>P round-robin counters are
        // global — otherwise many small inputs could each fill only the first sub-segment per pid.
        SegmentAssigner assigner = new SegmentAssigner(partitionSpec, partitionFunction);

        List<Path> dataFiles = reader.read();
        long inputRecordSize = computeInputRecordSize(dataFiles);

        Path splitDir = Files.createTempDirectory("segment-split-");
        Path outputDir = Files.createTempDirectory("segment-output-");

        List<SegmentInfo> results = new ArrayList<>();
        long inputRecordCount = 0;

        try {
            LOG.info("transient(core): split " + dataFiles.size()
                    + " input files into " + partitionSpec.count() + " segments...");
            Map<Integer, List<Path>> splitFiles =
                    splitInputFilesParallel(dataFiles, splitDir, partitionSpec, assigner);

            LOG.info("transient(core): split-phase on-disk footprint " + directorySizeBytes(splitDir)
                    + " bytes across " + partitionSpec.count() + " segments (inputs freed)");

            for (List<Path> paths : splitFiles.values()) {
                inputRecordCount += countRecordsInFiles(paths);
            }
            LOG.info("transient(core): read " + dataFiles.size() + " input files with total "
                    + inputRecordCount + " records and " + inputRecordSize + " bytes");

            results = buildSegmentsParallel(splitFiles, partitionSpec, tableName, outputDir);

        } catch (Exception e) {
            deleteDirectory(outputDir.toFile());
            throw e;
        } finally {
            deleteDirectory(splitDir.toFile());
            for (Path dataFile : dataFiles) {
                Files.deleteIfExists(dataFile);
            }
        }

        return new GenerationResult(results, inputRecordCount, inputRecordSize);
    }

    /**
     * Splits every input file in parallel.  Each file gets its own isolated
     * sub-directory and its own {@link FileSplitter} instance (the splitters are
     * stateful and not thread-safe, so one-per-file is required).  All splitters share
     * the single thread-safe {@link SegmentAssigner} so the S&gt;P round-robin counters
     * are global.  The per-file results are merged into a single
     * {@code segmentId → [paths]} map.
     */
    private Map<Integer, List<Path>> splitInputFilesParallel(
            List<Path> dataFiles, Path splitDir, PartitionSpec spec,
            SegmentAssigner assigner) throws Exception {

        int threads = Math.min(dataFiles.size(), splitParallelism);
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(threads, 1));

        List<CompletableFuture<Map<Integer, List<Path>>>> futures = new ArrayList<>();
        for (int i = 0; i < dataFiles.size(); i++) {
            final Path file = dataFiles.get(i);
            // Each file gets its own sub-directory so splitters never share output paths.
            final Path fileDir = splitDir.resolve("file-" + i);
            Files.createDirectories(fileDir);

            CompletableFuture<Map<Integer, List<Path>>> future = CompletableFuture.supplyAsync(() -> {
                FileSplitter splitter = createFileSplitter(spec, fileDir, assigner);
                try {
                    splitter.split(file);
                    splitter.close();
                    Map<Integer, List<Path>> splitFiles = splitter.splitFiles();
                    // Free the input copy as soon as it has been re-partitioned into split files.
                    // Nothing downstream reads the raw input again (record count/size are already
                    // accounted for), so this keeps split-phase disk at ~1x the dataset instead of
                    // holding every input alongside every split until the run ends.
                    deleteFileQuietly(file);
                    return splitFiles;
                } catch (Exception e) {
                    try { splitter.close(); } catch (Exception ignored) {}
                    throw new CompletionException(e);
                }
            }, executor);

            futures.add(future);
        }

        executor.shutdown();
        // Merge results from all futures
        Map<Integer, List<Path>> merged = new HashMap<>();
        try {
            for (CompletableFuture<Map<Integer, List<Path>>> future : futures) {
                Map<Integer, List<Path>> partial = future.get();
                for (Map.Entry<Integer, List<Path>> entry : partial.entrySet()) {
                    merged.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                          .addAll(entry.getValue());
                }
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw (cause instanceof Exception) ? (Exception) cause : new RuntimeException(cause);
        } finally {
            executor.shutdownNow();
        }
        return merged;
    }

    /**
     * Builds and uploads one segment per partition, all concurrently.
     */
    private List<SegmentInfo> buildSegmentsParallel(
            Map<Integer, List<Path>> splitFiles,
            PartitionSpec partitionSpec,
            String tableName,
            Path outputDir) throws Exception {

        String tableNameVal = tableName;
        int threads = Math.min(partitionSpec.count(), buildParallelism);
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(threads, 1));

        List<CompletableFuture<SegmentInfo>> futures = new ArrayList<>();
        for (int i = 0; i < partitionSpec.count(); i++) {
            final int partitionId = i;
            final String segmentName = tableNameVal + "_" + segmentKey + "_" + partitionId;
            final List<Path> partFiles =
                    splitFiles.getOrDefault(partitionId, Collections.emptyList());

            CompletableFuture<SegmentInfo> future = CompletableFuture.supplyAsync(() -> {
                LOG.info("transient(core): create segment " + segmentName);
                try (RecordReader nativeReader = createNativeRecordReader(partFiles)) {
                    BuildResult built = buildSegment(nativeReader, segmentName, outputDir);
                    long outputRecordCount = built.totalDocs();
                    long outputRecordSize = built.tarFile().length();

                    String remoteURI = writer.write(segmentName + ".tar.gz", built.tarFile().toPath());
                    LOG.info("transient(oss): upload result segment " + segmentName + " with " + outputRecordCount + " records and size " + outputRecordSize + " bytes" + " to " + remoteURI);
                    return new SegmentInfo(segmentName, remoteURI, built.tarFile().toPath(),
                            built.metadataFile() == null ? null : built.metadataFile().toPath(),
                            outputRecordCount, outputRecordSize);
                } catch (Exception e) {
                    throw new CompletionException(e);
                } finally {
                    // Free this partition's split files now that its segment is built; these paths
                    // are unique to this partition, so no other concurrent build needs them. The
                    // split dir drains as segment output accumulates, keeping peak disk ~1x.
                    for (Path partFile : partFiles) {
                        deleteFileQuietly(partFile);
                    }
                }
            }, executor);

            futures.add(future);
        }

        executor.shutdown();
        List<SegmentInfo> results = new ArrayList<>();
        try {
            for (CompletableFuture<SegmentInfo> future : futures) {
                results.add(future.get());
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw (cause instanceof Exception) ? (Exception) cause : new RuntimeException(cause);
        } finally {
            executor.shutdownNow();
        }
        return results;
    }

    /** Factory that picks the right {@link FileSplitter} based on {@link #inputFormat}. */
    private FileSplitter createFileSplitter(PartitionSpec spec, Path splitDir, SegmentAssigner assigner) {
        switch (inputFormat) {
            case "avro":
                return new AvroFileSplitter(spec, splitDir, schema, assigner);
            case "parquet":
                return new ParquetFileSplitter(spec, splitDir, schema, assigner);
            case "json":
                return new JsonFileSplitter(spec, splitDir, schema, assigner);
            default:
                throw new IllegalArgumentException("Unsupported input format: " + inputFormat);
        }
    }

    /**
     * Creates a Pinot {@link RecordReader} over all split files for one partition,
     * concatenating them via {@link ConcatenatingRecordReader}.
     */
    private RecordReader createNativeRecordReader(List<Path> partFiles) throws Exception {
        if (partFiles.isEmpty()) {
            return new EmptyRecordReader();
        }

        List<RecordReader> readers = new ArrayList<>();
        Set<String> fields = new HashSet<>(schema.getPhysicalColumnNames());

        for (Path file : partFiles) {
            readers.add(instantiateNativeReader(file, fields));
        }

        return readers.size() == 1 ? readers.get(0) : new ConcatenatingRecordReader(readers);
    }

    private RecordReader instantiateNativeReader(Path file, Set<String> fields) throws Exception {
        switch (inputFormat) {
            case "avro": {
                AvroRecordReader rr = new AvroRecordReader();
                rr.init(file.toFile(), fields, null);
                return rr;
            }
            case "parquet": {
                ParquetRecordReader rr = new ParquetRecordReader();
                rr.init(file.toFile(), fields, null);
                return rr;
            }
            case "json": {
                JSONRecordReader rr = new JSONRecordReader();
                rr.init(file.toFile(), fields, null);
                return rr;
            }
            default:
                throw new IllegalArgumentException("Unsupported format: " + inputFormat);
        }
    }

    private BuildResult buildSegment(RecordReader recordReader,
                                     String segmentName,
                                     Path outputDir) throws Exception {
        SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema, true);
        config.setOutDir(outputDir.toAbsolutePath().toString());
        config.setSegmentName(segmentName);

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(config, recordReader);
        driver.build();

        File segmentDir = outputDir.resolve(segmentName).toFile();
        File tarFile = outputDir.resolve(segmentName + ".tar.gz").toFile();
        createTarGz(segmentDir, tarFile);

        long totalDocs = readSegmentTotalDocs(segmentDir);
        File metadataFile = createMetadataTarGz(segmentDir, segmentName, outputDir);
        deleteDirectory(segmentDir);
        return new BuildResult(tarFile, metadataFile, totalDocs);
    }

    private PartitionSpec extractPartitionSpec() {
        String partitionColumn = null;
        int numPartitions = 1;

        SegmentPartitionConfig partitionConfig = tableConfig.getIndexingConfig() != null
                ? tableConfig.getIndexingConfig().getSegmentPartitionConfig() : null;

        if (partitionConfig != null) {
            Map<String, ColumnPartitionConfig> columnMap = partitionConfig.getColumnPartitionMap();
            if (columnMap != null && !columnMap.isEmpty()) {
                Map.Entry<String, ColumnPartitionConfig> entry =
                        columnMap.entrySet().iterator().next();
                partitionColumn = entry.getKey();
                numPartitions = entry.getValue().getNumPartitions();
            }
        }
        // S defaults to P (backward compatible) unless PINOT__SEGMENT_COUNT was configured.
        int resolvedSegmentCount = segmentCount > 0 ? segmentCount : numPartitions;
        return new PartitionSpec(partitionColumn, numPartitions, resolvedSegmentCount);
    }

    private long computeInputRecordSize(List<Path> dataFiles) {
        long total = 0;
        for (Path file : dataFiles) {
            try { total += Files.size(file); } catch (Exception ignored) {}
        }
        return total;
    }

    private long countRecordsInFiles(List<Path> files) {
        long count = 0;
        Set<String> fields = new HashSet<>(schema.getPhysicalColumnNames());
        for (Path file : files) {
            try (RecordReader rr = instantiateNativeReader(file, fields)) {
                while (rr.hasNext()) { rr.next(); count++; }
            } catch (Exception e) {
                LOG.warning("transient(core): could not count records in " + file
                        + ": " + e.getMessage());
            }
        }
        return count;
    }

    private long readSegmentTotalDocs(File segmentDir) {
        try {
            return new SegmentMetadataImpl(segmentDir).getTotalDocs();
        } catch (Exception e) {
            LOG.warning("transient(core): could not read segment metadata from "
                    + segmentDir + ": " + e.getMessage());
            return 0;
        }
    }

    private void createTarGz(File sourceDir, File outputFile) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(bos);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            addDirectoryToTar(taos, sourceDir, sourceDir.getName());
        }
    }

    private File createMetadataTarGz(File segmentDir, String segmentName, Path outputDir) throws Exception {
        File metadataDir = outputDir.resolve(segmentName + "-metadata").toFile();
        File metadataSegmentDir = new File(metadataDir, segmentName);
        if (!metadataSegmentDir.mkdirs()) {
            throw new java.io.IOException("Failed to create metadata staging dir: " + metadataSegmentDir);
        }

        Set<String> targetFiles = Set.of("metadata.properties", "creation.meta");

        try (var paths = Files.walk(segmentDir.toPath())) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> targetFiles.contains(path.getFileName().toString()))
                    .forEach(path -> {
                        try {
                            Path target = new File(metadataSegmentDir,path.getFileName().toString()).toPath();
                            Files.copy(path, target, java.nio.file.StandardCopyOption.COPY_ATTRIBUTES);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        File metadataTar = outputDir.resolve(segmentName + ".metadata.tar.gz").toFile();
        createTarGz(metadataSegmentDir, metadataTar);
        deleteDirectory(metadataDir);
        return metadataTar;
    }

    private void addDirectoryToTar(TarArchiveOutputStream taos,
                                   File source, String entryName) throws Exception {
        TarArchiveEntry entry = new TarArchiveEntry(source, entryName);
        taos.putArchiveEntry(entry);
        taos.closeArchiveEntry();
        File[] children = source.listFiles();
        if (children != null) {
            for (File child : children) {
                String childName = entryName + "/" + child.getName();
                if (child.isDirectory()) {
                    addDirectoryToTar(taos, child, childName);
                } else {
                    TarArchiveEntry fileEntry = new TarArchiveEntry(child, childName);
                    taos.putArchiveEntry(fileEntry);
                    Files.copy(child.toPath(), taos);
                    taos.closeArchiveEntry();
                }
            }
        }
    }

    /** Deletes a temp file best-effort; failures are logged, never propagated (runs on worker threads). */
    private void deleteFileQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (Exception e) {
            LOG.warning("transient(core): could not delete temp file " + path + ": " + e.getMessage());
        }
    }

    /** Best-effort recursive byte size of a directory tree, for observability logging. */
    private long directorySizeBytes(Path dir) {
        try (var paths = Files.walk(dir)) {
            return paths.filter(Files::isRegularFile).mapToLong(p -> {
                try { return Files.size(p); } catch (Exception e) { return 0L; }
            }).sum();
        } catch (Exception e) {
            return 0L;
        }
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) deleteDirectory(file);
                else file.delete();
            }
        }
        dir.delete();
    }

    private record BuildResult(File tarFile, File metadataFile, long totalDocs) {
    }
}