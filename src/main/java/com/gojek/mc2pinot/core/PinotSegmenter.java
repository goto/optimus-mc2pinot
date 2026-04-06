package com.gojek.mc2pinot.core;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.core.reader.PartitionFilteringRecordReader;
import com.gojek.mc2pinot.io.Reader;
import com.gojek.mc2pinot.io.Writer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public List<SegmentInfo> generateSegment() throws Exception {
        String tableName = tableConfig.getTableName();
        PartitionSpec partitionSpec = extractPartitionSpec();
        LOG.info("transient(core): start generating " + partitionSpec.count + " segments...");

        List<Path> dataFiles = reader.read();
        Path outputDir = Files.createTempDirectory("segment-output-");
        List<SegmentInfo> results = new ArrayList<>();

        try {
            for (int i = 0; i < partitionSpec.count; i++) {
                String segmentName = tableName + "_" + segmentKey + "_" + i;
                LOG.info("transient(core): create segment for segment " + segmentName);

                try (PartitionFilteringRecordReader filterReader = new PartitionFilteringRecordReader(
                        dataFiles, inputFormat, schema.getPhysicalColumnNames(),
                        i, partitionSpec.count, partitionSpec.column, partitionFunction)) {

                    File tarFile = buildSegment(filterReader, segmentName, outputDir);
                    LOG.info("transient(oss): upload result segment " + segmentName);
                    String remoteURI = writer.write(segmentName + ".tar.gz", tarFile.toPath());
                    results.add(new SegmentInfo(segmentName, remoteURI, tarFile.toPath()));
                }
            }
        } catch (Exception e) {
            deleteDirectory(outputDir.toFile());
            throw e;
        } finally {
            for (Path dataFile : dataFiles) {
                Files.deleteIfExists(dataFile);
            }
        }

        return results;
    }

    private PartitionSpec extractPartitionSpec() {
        String partitionColumn = null;
        int numPartitions = 1;

        SegmentPartitionConfig partitionConfig = tableConfig.getIndexingConfig() != null
                ? tableConfig.getIndexingConfig().getSegmentPartitionConfig() : null;

        if (partitionConfig != null) {
            Map<String, ColumnPartitionConfig> columnMap = partitionConfig.getColumnPartitionMap();
            if (columnMap != null && !columnMap.isEmpty()) {
                Map.Entry<String, ColumnPartitionConfig> entry = columnMap.entrySet().iterator().next();
                partitionColumn = entry.getKey();
                numPartitions = entry.getValue().getNumPartitions();
            }
        }

        return new PartitionSpec(partitionColumn, numPartitions);
    }

    private File buildSegment(RecordReader recordReader, String segmentName, Path outputDir) throws Exception {
        SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
        config.setOutDir(outputDir.toAbsolutePath().toString());
        config.setSegmentName(segmentName);

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(config, recordReader);
        driver.build();

        File segmentDir = outputDir.resolve(segmentName).toFile();
        File tarFile = outputDir.resolve(segmentName + ".tar.gz").toFile();
        createTarGz(segmentDir, tarFile);
        deleteDirectory(segmentDir);

        return tarFile;
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

    private void addDirectoryToTar(TarArchiveOutputStream taos, File source, String entryName) throws Exception {
        TarArchiveEntry entry = new TarArchiveEntry(source, entryName);
        taos.putArchiveEntry(entry);
        taos.closeArchiveEntry();
        File[] children = source.listFiles();
        if (children != null) {
            for (File child : children) {
                String childEntryName = entryName + "/" + child.getName();
                if (child.isDirectory()) {
                    addDirectoryToTar(taos, child, childEntryName);
                } else {
                    TarArchiveEntry fileEntry = new TarArchiveEntry(child, childEntryName);
                    taos.putArchiveEntry(fileEntry);
                    java.nio.file.Files.copy(child.toPath(), taos);
                    taos.closeArchiveEntry();
                }
            }
        }
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    private record PartitionSpec(String column, int count) {
    }
}
