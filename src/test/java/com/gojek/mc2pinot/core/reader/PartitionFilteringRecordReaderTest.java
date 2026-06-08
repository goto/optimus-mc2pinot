package com.gojek.mc2pinot.core.reader;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PartitionFilteringRecordReaderTest {

    /** Simple deterministic partition: Integer.parseInt(value) % numPartitions */
    private static final PartitionFunction MODULO = (value, n) -> {
        if (value == null || value.isEmpty()) return 0;
        try {
            return Math.floorMod(Integer.parseInt(value), n);
        } catch (NumberFormatException e) {
            return Math.floorMod(value.hashCode(), n);
        }
    };

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Path writeJsonLines(String fileName, String... lines) throws IOException {
        Path file = tempDir.resolve(fileName);
        Files.writeString(file, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        return file;
    }

    private List<GenericRow> readAll(PartitionFilteringRecordReader reader) throws IOException {
        List<GenericRow> rows = new ArrayList<>();
        while (reader.hasNext()) {
            rows.add(reader.next());
        }
        return rows;
    }

    private Set<String> idColumn() {
        return Set.of("id");
    }

    // -------------------------------------------------------------------------
    // Tests: no-partition-filter fast path
    // -------------------------------------------------------------------------

    @Test
    void shouldReadAllRowsWhenPartitionColumnIsNull() throws Exception {
        Path file = writeJsonLines("data.json",
                "{\"id\":\"0\"}",
                "{\"id\":\"1\"}",
                "{\"id\":\"2\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 2, null, MODULO)) {

            List<GenericRow> rows = readAll(reader);
            assertEquals(3, rows.size());
        }
    }

    @Test
    void shouldReadAllRowsWhenTotalPartitionsIsOne() throws Exception {
        Path file = writeJsonLines("data.json",
                "{\"id\":\"0\"}",
                "{\"id\":\"1\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 1, "id", MODULO)) {

            List<GenericRow> rows = readAll(reader);
            assertEquals(2, rows.size());
        }
    }

    // -------------------------------------------------------------------------
    // Tests: partition filtering
    // -------------------------------------------------------------------------

    @Test
    void shouldReturnOnlyRowsMatchingTargetPartition() throws Exception {
        // ids 0,2,4 → partition 0; ids 1,3 → partition 1
        Path file = writeJsonLines("data.json",
                "{\"id\":\"0\"}",
                "{\"id\":\"1\"}",
                "{\"id\":\"2\"}",
                "{\"id\":\"3\"}",
                "{\"id\":\"4\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 2, "id", MODULO)) {

            List<GenericRow> rows = readAll(reader);
            assertEquals(3, rows.size());
            for (GenericRow row : rows) {
                assertEquals(0, MODULO.partition((String) row.getValue("id"), 2));
            }
        }
    }

    @Test
    void shouldReturnOnlyRowsMatchingPartitionOne() throws Exception {
        Path file = writeJsonLines("data.json",
                "{\"id\":\"0\"}",
                "{\"id\":\"1\"}",
                "{\"id\":\"2\"}",
                "{\"id\":\"3\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 1, 2, "id", MODULO)) {

            List<GenericRow> rows = readAll(reader);
            assertEquals(2, rows.size());
            for (GenericRow row : rows) {
                assertEquals(1, MODULO.partition((String) row.getValue("id"), 2));
            }
        }
    }

    @Test
    void shouldReturnNoRowsWhenNoneMatchPartition() throws Exception {
        // All ids are even → only partition 0 gets rows; partition 1 gets none
        Path file = writeJsonLines("data.json",
                "{\"id\":\"0\"}",
                "{\"id\":\"2\"}",
                "{\"id\":\"4\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 1, 2, "id", MODULO)) {

            assertFalse(reader.hasNext());
        }
    }

    // -------------------------------------------------------------------------
    // Tests: multiple source files (exercises async pre-open)
    // -------------------------------------------------------------------------

    @Test
    void shouldReadAcrossMultipleFiles() throws Exception {
        Path file1 = writeJsonLines("data1.json", "{\"id\":\"0\"}", "{\"id\":\"1\"}");
        Path file2 = writeJsonLines("data2.json", "{\"id\":\"2\"}", "{\"id\":\"3\"}");
        Path file3 = writeJsonLines("data3.json", "{\"id\":\"4\"}", "{\"id\":\"5\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file1, file2, file3), "json", idColumn(), 0, 1, null, MODULO)) {

            List<GenericRow> rows = readAll(reader);
            assertEquals(6, rows.size());
        }
    }

    @Test
    void shouldFilterAcrossMultipleFilesWithPartitioning() throws Exception {
        Path file1 = writeJsonLines("data1.json", "{\"id\":\"0\"}", "{\"id\":\"1\"}");
        Path file2 = writeJsonLines("data2.json", "{\"id\":\"2\"}", "{\"id\":\"3\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file1, file2), "json", idColumn(), 0, 2, "id", MODULO)) {

            List<GenericRow> rows = readAll(reader);
            // ids 0 and 2 → partition 0
            assertEquals(2, rows.size());
            for (GenericRow row : rows) {
                assertEquals(0, MODULO.partition((String) row.getValue("id"), 2));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Tests: empty source list
    // -------------------------------------------------------------------------

    @Test
    void shouldHandleEmptySourceList() throws Exception {
        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                Collections.emptyList(), "json", idColumn(), 0, 2, "id", MODULO)) {

            assertFalse(reader.hasNext());
        }
    }

    // -------------------------------------------------------------------------
    // Tests: rewind
    // -------------------------------------------------------------------------

    @Test
    void shouldRewindAndReReadAllRows() throws Exception {
        Path file = writeJsonLines("data.json",
                "{\"id\":\"0\"}",
                "{\"id\":\"1\"}",
                "{\"id\":\"2\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 1, null, MODULO)) {

            List<GenericRow> firstPass = readAll(reader);
            assertEquals(3, firstPass.size());

            reader.rewind();

            List<GenericRow> secondPass = readAll(reader);
            assertEquals(3, secondPass.size());
        }
    }

    @Test
    void shouldRewindAndApplyPartitionFilterAgain() throws Exception {
        Path file = writeJsonLines("data.json",
                "{\"id\":\"0\"}",
                "{\"id\":\"1\"}",
                "{\"id\":\"2\"}",
                "{\"id\":\"3\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 2, "id", MODULO)) {

            List<GenericRow> firstPass = readAll(reader);
            assertEquals(2, firstPass.size());

            reader.rewind();

            List<GenericRow> secondPass = readAll(reader);
            assertEquals(2, secondPass.size());
        }
    }

    // -------------------------------------------------------------------------
    // Tests: hasNext idempotency
    // -------------------------------------------------------------------------

    @Test
    void hasNextShouldBeIdempotent() throws Exception {
        Path file = writeJsonLines("data.json", "{\"id\":\"0\"}", "{\"id\":\"1\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 1, null, MODULO)) {

            assertTrue(reader.hasNext());
            assertTrue(reader.hasNext());
            assertTrue(reader.hasNext());

            reader.next();

            assertTrue(reader.hasNext());
            reader.next();

            assertFalse(reader.hasNext());
            assertFalse(reader.hasNext());
        }
    }

    // -------------------------------------------------------------------------
    // Tests: next(GenericRow reuse) returns a non-null row
    // -------------------------------------------------------------------------

    @Test
    void nextWithReuseShouldReturnRow() throws Exception {
        Path file = writeJsonLines("data.json", "{\"id\":\"42\"}");

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 1, null, MODULO)) {

            assertTrue(reader.hasNext());
            GenericRow row = reader.next(new GenericRow());
            assertNotNull(row);
        }
    }

    // -------------------------------------------------------------------------
    // Tests: close is safe to call multiple times / on empty reader
    // -------------------------------------------------------------------------

    @Test
    void closeShouldBeIdempotent() throws Exception {
        Path file = writeJsonLines("data.json", "{\"id\":\"0\"}");

        PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 1, null, MODULO);

        assertDoesNotThrow(() -> {
            reader.close();
            reader.close();
        });
    }

    @Test
    void closeShouldWorkOnEmptySourceList() throws Exception {
        PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                Collections.emptyList(), "json", idColumn(), 0, 1, null, MODULO);

        assertDoesNotThrow(reader::close);
    }

    // -------------------------------------------------------------------------
    // Tests: row pool — rows rejected by partition filter must not bleed over
    // -------------------------------------------------------------------------

    @Test
    void rowPoolShouldNotCauseDataCorruption() throws Exception {
        // Write rows such that only the last one matches the target partition.
        // The pool will recycle the rejected ones; the returned row must hold
        // the correct data from the matching row.
        Path file = writeJsonLines("data.json",
                "{\"id\":\"1\"}",  // partition 1 (rejected for target=0)
                "{\"id\":\"3\"}",  // partition 1 (rejected)
                "{\"id\":\"4\"}"); // partition 0 (accepted)

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 0, 2, "id", MODULO)) {

            assertTrue(reader.hasNext());
            GenericRow row = reader.next();
            assertEquals("4", row.getValue("id").toString());
            assertFalse(reader.hasNext());
        }
    }

    // -------------------------------------------------------------------------
    // Tests: single file with many partitions
    // -------------------------------------------------------------------------

    @Test
    void shouldHandleManyPartitions() throws Exception {
        // 10 rows, targeting partition 3 out of 10
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("{\"id\":\"").append(i).append("\"}\n");
        }
        Path file = tempDir.resolve("data.json");
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);

        try (PartitionFilteringRecordReader reader = new PartitionFilteringRecordReader(
                List.of(file), "json", idColumn(), 3, 10, "id", MODULO)) {

            List<GenericRow> rows = readAll(reader);
            assertEquals(1, rows.size());
            assertEquals("3", rows.get(0).getValue("id").toString());
        }
    }
}

