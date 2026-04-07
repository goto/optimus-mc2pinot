package com.gojek.mc2pinot.core.reader;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

public class PartitionFilteringRecordReader implements RecordReader {

    private final List<Path> sourceFiles;
    private final String inputFormat;
    private final Set<String> columns;
    private final int targetPartitionId;
    private final int totalPartitions;
    private final String partitionColumn;
    private final PartitionFunction partitionFunction;

    private int currentFileIndex = 0;
    private RecordReader currentReader;
    private GenericRow prefetchedRow;

    public PartitionFilteringRecordReader(List<Path> sourceFiles, String inputFormat,
                                          Set<String> columns, int targetPartitionId,
                                          int totalPartitions, String partitionColumn,
                                          PartitionFunction partitionFunction) throws Exception {
        this.sourceFiles = sourceFiles;
        this.inputFormat = inputFormat;
        this.columns = columns;
        this.targetPartitionId = targetPartitionId;
        this.totalPartitions = totalPartitions;
        this.partitionColumn = partitionColumn;
        this.partitionFunction = partitionFunction;
        this.currentReader = openReader(0);
    }

    @Override
    public void init(File dataFile, Set<String> fieldsToRead, RecordReaderConfig config) {
    }

    @Override
    public boolean hasNext() {
        if (prefetchedRow != null) {
            return true;
        }
        while (currentReader != null) {
            try {
                if (currentReader.hasNext()) {
                    GenericRow row = currentReader.next(new GenericRow());
                    if (matchesPartition(row)) {
                        prefetchedRow = row;
                        return true;
                    }
                } else {
                    advanceFile();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public GenericRow next() throws IOException {
        return next(new GenericRow());
    }

    @Override
    public GenericRow next(GenericRow reuse) throws IOException {
        if (prefetchedRow == null) {
            hasNext();
        }
        GenericRow result = prefetchedRow;
        prefetchedRow = null;
        return result;
    }

    @Override
    public void rewind() throws IOException {
        if (currentReader != null) {
            currentReader.close();
        }
        currentFileIndex = 0;
        prefetchedRow = null;
        try {
            currentReader = openReader(0);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
    }

    private boolean matchesPartition(GenericRow row) {
        if (partitionColumn == null || totalPartitions <= 1) {
            return true;
        }
        Object value = row.getValue(partitionColumn);
        String str = value != null ? value.toString() : "";
        return partitionFunction.partition(str, totalPartitions) == targetPartitionId;
    }

    private void advanceFile() throws IOException {
        currentReader.close();
        currentReader = null;
        currentFileIndex++;
        try {
            currentReader = openReader(currentFileIndex);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private RecordReader openReader(int index) throws Exception {
        if (index >= sourceFiles.size()) {
            return null;
        }
        File file = sourceFiles.get(index).toAbsolutePath().toFile();
        RecordReader r = switch (inputFormat) {
            case "parquet" -> new ParquetRecordReader();
            case "avro" -> new AvroRecordReader();
            default -> new JSONRecordReader();
        };
        r.init(file, columns, null);
        return r;
    }
}

