package com.gojek.mc2pinot.core;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.io.Reader;
import com.gojek.mc2pinot.io.Writer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PinotSegmenterTest {

    @Mock
    private Reader reader;

    @Mock
    private Writer writer;

    @Mock
    private PartitionFunction partitionFunction;

    @TempDir
    Path tempDir;

    private Schema schema;
    private TableConfig tableConfig;
    private TableConfig partitionedTableConfig;  // 2 partitions on "id"

    /** Deterministic modulo partition: Integer.parseInt(id) % n */
    private static final PartitionFunction MODULO = (value, n) ->
            Math.floorMod(Integer.parseInt(value), n);

    @BeforeEach
    void setUp() throws Exception {
        schema = loadSchema("/test-schema.json");
        tableConfig = loadTableConfig("/test-tableConfig.json");
        partitionedTableConfig = loadTableConfig("/test-partitioned-tableConfig.json");
    }

    @Test
    void shouldGenerateSegmentFromJsonData() throws Exception {
        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "12345", "json", schema, tableConfig, partitionFunction);

        GenerationResult result = segmenter.generateSegment();
        List<SegmentInfo> segments = result.segments();

        assertEquals(1, segments.size());
        SegmentInfo seg = segments.get(0);
        assertEquals("test_table_OFFLINE_12345_0", seg.segmentName());
        assertTrue(seg.remoteURI().startsWith("oss://bucket/segments/"));
        assertTrue(seg.remoteURI().endsWith(".tar.gz"));
        assertNotNull(seg.localPath());
        assertTrue(Files.exists(seg.localPath()));
        assertEquals(2, seg.outputRecordCount());
        assertTrue(seg.outputRecordSize() > 0);
        assertEquals(2, result.inputRecordCount());
        assertTrue(result.inputRecordSize() > 0);

        verify(reader).read();
        verify(writer).write(eq("test_table_OFFLINE_12345_0.tar.gz"), any(Path.class));
    }

    @Test
    void shouldGenerateMultipleSegmentsWithNoPartitionConfig() throws Exception {
        Path dataFile1 = createTestDataFile("data1.json");
        Path dataFile2 = createTestDataFile("data2.json");
        when(reader.read()).thenReturn(List.of(dataFile1, dataFile2));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "99", "json", schema, tableConfig, partitionFunction);

        GenerationResult result = segmenter.generateSegment();

        assertEquals(1, result.segments().size());
        verify(writer, times(1)).write(anyString(), any(Path.class));
    }

    @Test
    void shouldPropagateReaderException() throws Exception {
        when(reader.read()).thenThrow(new IOException("OSS connection failed"));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "key", "json", schema, tableConfig, partitionFunction);

        assertThrows(IOException.class, segmenter::generateSegment);
    }

    @Test
    void shouldPropagateWriterException() throws Exception {
        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenThrow(new IOException("OSS upload failed"));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "key", "json", schema, tableConfig, partitionFunction);

        assertThrows(IOException.class, segmenter::generateSegment);
    }

    // -----------------------------------------------------------------------
    // Parallel segment generation tests
    // -----------------------------------------------------------------------

    /**
     * Two partitions are built in parallel. Each partition gets exactly the rows
     * whose id satisfies id % 2 == partitionId.
     * ids: "0","2" → partition 0 (2 records); "1","3" → partition 1 (2 records).
     */
    @Test
    void shouldGenerateTwoSegmentsInParallelWithPartitioning() throws Exception {
        Path dataFile = createPartitionedDataFile("partitioned.json");
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "ts1", "json", schema, partitionedTableConfig, MODULO);

        GenerationResult result = segmenter.generateSegment();
        List<SegmentInfo> segments = result.segments();

        assertEquals(2, segments.size());

        // Both segments must have been written
        Set<String> names = new HashSet<>();
        for (SegmentInfo seg : segments) {
            names.add(seg.segmentName());
            assertTrue(seg.outputRecordCount() > 0,
                    "Expected records in segment " + seg.segmentName());
            assertTrue(seg.remoteURI().startsWith("oss://bucket/segments/"));
        }
        assertTrue(names.contains("test_table_OFFLINE_ts1_0"));
        assertTrue(names.contains("test_table_OFFLINE_ts1_1"));

        // writer.write must be called once per segment
        verify(writer, times(2)).write(anyString(), any(Path.class));
        assertEquals(4, result.inputRecordCount());
    }

    /**
     * The result list must preserve submission order regardless of which thread
     * finishes first — partition_0 must always be at index 0.
     */
    @Test
    void shouldPreserveSegmentResultOrdering() throws Exception {
        Path dataFile = createPartitionedDataFile("order.json");
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "ord", "json", schema, partitionedTableConfig, MODULO);

        List<SegmentInfo> segments = segmenter.generateSegment().segments();

        assertEquals(2, segments.size());
        assertEquals("test_table_OFFLINE_ord_0", segments.get(0).segmentName());
        assertEquals("test_table_OFFLINE_ord_1", segments.get(1).segmentName());
    }

    /**
     * When the writer throws from inside a parallel task the exception must be
     * unwrapped and propagated to the caller as the original type.
     */
    @Test
    void shouldPropagateWriterExceptionFromParallelTask() throws Exception {
        Path dataFile = createPartitionedDataFile("fail.json");
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenThrow(new IOException("OSS upload failed"));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "fail", "json", schema, partitionedTableConfig, MODULO);

        assertThrows(IOException.class, segmenter::generateSegment);
    }

    /**
     * A single-partition table still uses the parallel code path (pool size 1)
     * and must behave identically to the old sequential path.
     */
    @Test
    void shouldWorkCorrectlyWithSinglePartition() throws Exception {
        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "single", "json", schema, tableConfig, partitionFunction);

        GenerationResult result = segmenter.generateSegment();

        assertEquals(1, result.segments().size());
        assertEquals("test_table_OFFLINE_single_0", result.segments().get(0).segmentName());
        verify(writer, times(1)).write(anyString(), any(Path.class));
    }

    /**
     * writer.write() may be called concurrently from multiple threads.
     * This test verifies that concurrent invocations all complete and each
     * segment is uploaded exactly once.
     */
    @Test
    void shouldCallWriterExactlyOncePerSegment() throws Exception {
        Path dataFile = createPartitionedDataFile("concurrent.json");
        when(reader.read()).thenReturn(List.of(dataFile));

        AtomicInteger callCount = new AtomicInteger();
        when(writer.write(anyString(), any(Path.class))).thenAnswer(inv -> {
            callCount.incrementAndGet();
            return "oss://bucket/segments/" + inv.getArgument(0);
        });

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "cct", "json", schema, partitionedTableConfig, MODULO);

        segmenter.generateSegment();

        assertEquals(2, callCount.get(),
                "writer.write must be called exactly once per segment");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * 4 rows with numeric ids: 0,1,2,3.
     * With MODULO and 2 partitions: ids 0,2 → partition 0; ids 1,3 → partition 1.
     */
    private Path createPartitionedDataFile(String name) throws IOException {
        Path file = tempDir.resolve(name);
        String rows = String.join("\n",
                "{\"id\":\"0\",\"name\":\"Alice\",\"value\":100,\"ts\":1700000000000}",
                "{\"id\":\"1\",\"name\":\"Bob\",\"value\":200,\"ts\":1700000000000}",
                "{\"id\":\"2\",\"name\":\"Charlie\",\"value\":300,\"ts\":1700000000000}",
                "{\"id\":\"3\",\"name\":\"Dave\",\"value\":400,\"ts\":1700000000000}") + "\n";
        Files.writeString(file, rows, StandardCharsets.UTF_8);
        return file;
    }

    private Path createTestDataFile() throws IOException {
        return createTestDataFile("test-data.json");
    }

    private Path createTestDataFile(String name) throws IOException {
        Path file = tempDir.resolve(name);
        String line1 = "{\"id\":\"a\",\"name\":\"Alice\",\"value\":100,\"ts\":1700000000000}";
        String line2 = "{\"id\":\"b\",\"name\":\"Bob\",\"value\":200,\"ts\":1700000000000}";
        Files.writeString(file, line1 + "\n" + line2 + "\n", StandardCharsets.UTF_8);
        return file;
    }

    private Schema loadSchema(String resource) throws Exception {
        try (InputStream is = getClass().getResourceAsStream(resource)) {
            return Schema.fromInputStream(is);
        }
    }

    private TableConfig loadTableConfig(String resource) throws Exception {
        try (InputStream is = getClass().getResourceAsStream(resource)) {
            String json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return JsonUtils.stringToObject(json, TableConfig.class);
        }
    }
}

