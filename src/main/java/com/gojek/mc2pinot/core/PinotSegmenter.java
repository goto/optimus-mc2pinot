package com.gojek.mc2pinot.core;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.core.partition.PartitionSpec;
import com.gojek.mc2pinot.core.reader.ConcatenatingRecordReader;
import com.gojek.mc2pinot.core.reader.EmptyRecordReader;
import com.gojek.mc2pinot.core.splitter.AvroFileSplitter;
import com.gojek.mc2pinot.core.splitter.FileSplitter;
import com.gojek.mc2pinot.core.splitter.JsonFileSplitter;
import com.gojek.mc2pinot.core.splitter.ParquetFileSplitter;
import com.gojek.mc2pinot.io.Reader;
import com.gojek.mc2pinot.io.Writer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.*;
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

    public PinotSegmenter(Reader reader, Writer writer, String segmentKey,
                          String inputFormat, Schema schema, TableConfig tableConfig,
                          PartitionFunction partitionFunction) {
        this.reader = reader;
        this.writer = writer;
        this.segmentKey = segmentKey;
        this.inputFormat = inputFormat.toLowerCase();
        this.schema = schema;
        this.tableConfig = tableConfig;
        this.partitionFunction = partitionFunction;
    }

    public GenerationResult generateSegment() throws Exception {
        String tableName = tableConfig.getTableName();
        PartitionSpec partitionSpec = extractPartitionSpec();

        LOG.info("transient(core): start generating " + partitionSpec.count() + " segments...");

        List<Path> dataFiles = reader.read();
        long inputRecordSize = computeInputRecordSize(dataFiles);

        Path splitDir = Files.createTempDirectory("segment-split-");
        Path outputDir = Files.createTempDirectory("segment-output-");

        List<SegmentInfo> results = new ArrayList<>();
        long inputRecordCount = 0;

        try {
            LOG.info("transient(core): split input files into " + partitionSpec.count() + " partitions...");
            Map<Integer, List<Path>> splitFiles = splitInputFiles(
                    dataFiles, splitDir, partitionSpec);

            for (List<Path> paths : splitFiles.values()) {
                inputRecordCount += countRecordsInFiles(paths);
            }

            LOG.info("transient(core): read " + dataFiles.size() + " input files with total "
                    + inputRecordCount + " records and " + inputRecordSize + " bytes" );

            for (int i = 0; i < partitionSpec.count(); i++) {
                String segmentName = tableName + "_" + segmentKey + "_" + i;
                LOG.info("transient(core): create segment for segment " + segmentName);

                List<Path> partFiles = splitFiles.getOrDefault(i, Collections.emptyList());

                try (RecordReader nativeReader = createNativeRecordReader(partFiles)) {
                    BuildResult built = buildSegment(nativeReader, segmentName, outputDir);
                    long outputRecordCount = built.totalDocs();
                    long outputRecordSize = built.tarFile().length();

                    String remoteURI = writer.write(segmentName + ".tar.gz", built.tarFile().toPath());
                    LOG.info("transient(oss): upload result segment " + segmentName + " with " + outputRecordCount + " records and size " + outputRecordSize + " bytes" + " to " + remoteURI);
                    results.add(new SegmentInfo(segmentName, remoteURI, built.tarFile().toPath(),
                            built.metadataFile() == null ? null : built.metadataFile().toPath(),
                            outputRecordCount, outputRecordSize));
                }
            }

        } catch (Exception e) {
            deleteDirectory(outputDir.toFile());
            throw e;
        } finally {
            // clean up split temp files and original downloads
            deleteDirectory(splitDir.toFile());
            for (Path dataFile : dataFiles) {
                Files.deleteIfExists(dataFile);
            }
        }

        return new GenerationResult(results, inputRecordCount, inputRecordSize);
    }

    /**
     * Reads every input file once and fans out rows into per-partition temp files.
     *
     * @return map from partitionId → list of split temp files for that partition
     */
    private Map<Integer, List<Path>> splitInputFiles(
            List<Path> dataFiles, Path splitDir, PartitionSpec spec) throws Exception {

        FileSplitter splitter = createFileSplitter(spec, splitDir);
        try {
            for (Path file : dataFiles) {
                splitter.split(file);
            }
        } finally {
            splitter.close();
        }
        return splitter.splitFiles();
    }

    /**
     * Factory that picks the right {@link FileSplitter} based on {@link #inputFormat}.
     */
    private FileSplitter createFileSplitter(PartitionSpec spec, Path splitDir) {
        switch (inputFormat) {
            case "avro":
                return new AvroFileSplitter(spec, splitDir, schema, partitionFunction);
            case "parquet":
                return new ParquetFileSplitter(spec, splitDir, schema, partitionFunction);
            case "json":
                return new JsonFileSplitter(spec, splitDir, schema, partitionFunction);
            default:
                throw new IllegalArgumentException("Unsupported input format: " + inputFormat);
        }
    }

    /**
     * Creates a built-in Pinot {@link RecordReader} for all split files of one
     * partition, concatenating them via a simple {@link ConcatenatingRecordReader}.
     * If the list is empty an empty reader is returned.
     */
    private RecordReader createNativeRecordReader(List<Path> partFiles) throws Exception {
        if (partFiles.isEmpty()) {
            return new EmptyRecordReader();
        }

        List<RecordReader> readers = new ArrayList<>();
        Set<String> fields = new HashSet<>(schema.getPhysicalColumnNames());

        for (Path file : partFiles) {
            RecordReader rr = instantiateNativeReader(file, fields);
            readers.add(rr);
        }

        return readers.size() == 1
                ? readers.get(0)
                : new ConcatenatingRecordReader(readers);
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
        return new PartitionSpec(partitionColumn, numPartitions);
    }

    private long computeInputRecordSize(List<Path> dataFiles) {
        long total = 0;
        for (Path file : dataFiles) {
            try { total += Files.size(file); } catch (Exception ignored) {}
        }
        return total;
    }

    /**
     * Counts rows in a list of already-split files of the current format.
     * Cheap: files are already small (one partition's worth of data).
     */
    private long countRecordsInFiles(List<Path> files) {
        long count = 0;
        Set<String> fields = new HashSet<>(schema.getPhysicalColumnNames());
        for (Path file : files) {
            try (RecordReader rr = instantiateNativeReader(file, fields)) {
                while (rr.hasNext()) { rr.next(); count++; }
            } catch (Exception e) {
                LOG.warning("transient(core): could not count records in " + file + ": " + e.getMessage());
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