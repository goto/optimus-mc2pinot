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
import java.util.ArrayDeque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PartitionFilteringRecordReader implements RecordReader {

    private static final int PREFETCH_BUFFER_SIZE = 512;

    private final List<Path> sourceFiles;
    private final String inputFormat;
    private final Set<String> columns;
    private final int targetPartitionId;
    private final int totalPartitions;
    private final String partitionColumn;
    private final PartitionFunction partitionFunction;

    // Row pool to avoid allocating new GenericRow on every read
    private final ArrayDeque<GenericRow> rowPool = new ArrayDeque<>(PREFETCH_BUFFER_SIZE);

    private int currentFileIndex = 0;
    private RecordReader currentReader;
    private RecordReader nextReader;       // pre-opened next file
    private Future<?> nextReaderFuture;    // async open task
    private ExecutorService prefetchExecutor;

    private GenericRow prefetchedRow;

    // Cached result of partitionColumn == null || totalPartitions <= 1
    private final boolean noPartitionFilter;

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
        this.noPartitionFilter = (partitionColumn == null || totalPartitions <= 1);

        if (!sourceFiles.isEmpty()) {
            this.prefetchExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "record-reader-prefetch");
                t.setDaemon(true);
                return t;
            });
            this.currentReader = openReader(0);
            scheduleNextReaderOpen(1);
        }
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
                    // Reuse a pooled row instead of allocating a new one every time
                    GenericRow candidate = borrowRow();
                    GenericRow row = currentReader.next(candidate);
                    if (matchesPartition(row)) {
                        prefetchedRow = row;
                        return true;
                    } else {
                        // Return the row to the pool; it wasn't used
                        returnRow(row);
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
        closeCurrentReader();
        cancelPendingNextReader();
        currentFileIndex = 0;
        prefetchedRow = null;
        rowPool.clear();
        try {
            currentReader = openReader(0);
            scheduleNextReaderOpen(1);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        closeCurrentReader();
        cancelPendingNextReader();
        if (prefetchExecutor != null) {
            prefetchExecutor.shutdownNow();
        }
    }

    // --- Partition check ---

    private boolean matchesPartition(GenericRow row) {
        if (noPartitionFilter) {
            return true;
        }
        Object value = row.getValue(partitionColumn);
        // Avoid toString() when already a String
        String str = (value instanceof String) ? (String) value
                : (value != null ? value.toString() : "");
        return partitionFunction.partition(str, totalPartitions) == targetPartitionId;
    }

    // --- File advancement with pre-opened next reader ---

    private void advanceFile() throws IOException {
        closeCurrentReader();
        currentFileIndex++;

        // Swap in the pre-opened next reader (may block briefly if it isn't ready yet)
        if (nextReaderFuture != null) {
            try {
                nextReaderFuture.get(); // wait for async open to finish
            } catch (Exception e) {
                throw new IOException("Failed to open next reader", e);
            }
            nextReaderFuture = null;
        }
        currentReader = nextReader;
        nextReader = null;

        // Kick off opening the one after that
        scheduleNextReaderOpen(currentFileIndex + 1);
    }

    private void scheduleNextReaderOpen(int index) {
        if (prefetchExecutor == null || index >= sourceFiles.size()) {
            return;
        }
        nextReaderFuture = prefetchExecutor.submit(() -> {
            try {
                nextReader = openReader(index);
            } catch (Exception e) {
                throw new RuntimeException("Failed to pre-open reader at index " + index, e);
            }
        });
    }

    // --- Row pool (avoids per-row GC pressure) ---

    private GenericRow borrowRow() {
        GenericRow r = rowPool.poll();
        return (r != null) ? r : new GenericRow();
    }

    private void returnRow(GenericRow row) {
        if (rowPool.size() < PREFETCH_BUFFER_SIZE) {
            row.clear();
            rowPool.offer(row);
        }
    }

    // --- Reader lifecycle helpers ---

    private void closeCurrentReader() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
    }

    private void cancelPendingNextReader() throws IOException {
        if (nextReaderFuture != null) {
            nextReaderFuture.cancel(true);
            nextReaderFuture = null;
        }
        if (nextReader != null) {
            nextReader.close();
            nextReader = null;
        }
    }

    private RecordReader openReader(int index) throws Exception {
        if (index >= sourceFiles.size()) {
            return null;
        }
        File file = sourceFiles.get(index).toAbsolutePath().toFile();
        RecordReader r = switch (inputFormat) {
            case "parquet" -> new ParquetRecordReader();
            case "avro"    -> new AvroRecordReader();
            default        -> new JSONRecordReader();
        };
        r.init(file, columns, null);
        return r;
    }
}
