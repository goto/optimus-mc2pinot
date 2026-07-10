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
import java.util.List;

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

    @TempDir
    Path tempDir;

    private Schema schema;
    private TableConfig tableConfig;
    private TableConfig partitionedTableConfig;

    @BeforeEach
    void setUp() throws Exception {
        schema = loadSchema("/test-schema.json");
        tableConfig = loadTableConfig("/test-tableConfig.json");
        partitionedTableConfig = loadTableConfig("/test-tableConfig-partitioned.json");
    }

    // ── single-partition (no partition config) ────────────────────────────────

    @Test
    void shouldGenerateSegmentFromJsonData() throws Exception {
        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "12345", "json", schema, tableConfig,
                /* partitionFunction not needed for non-partitioned */ null);

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

        // Incremental cleanup: the input file is deleted once it has been split, not held
        // on disk until the run ends.
        assertFalse(Files.exists(dataFile));

        verify(reader).read();
        verify(writer).write(eq("test_table_OFFLINE_12345_0.tar.gz"), any(Path.class));
    }

    @Test
    void shouldMergeMultipleInputFilesIntoOneSegmentWhenNoPartitionConfig() throws Exception {
        Path dataFile1 = createTestDataFile("data1.json");
        Path dataFile2 = createTestDataFile("data2.json");
        when(reader.read()).thenReturn(List.of(dataFile1, dataFile2));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "99", "json", schema, tableConfig, null);

        GenerationResult result = segmenter.generateSegment();

        // Both files go to partition-0 → exactly one segment
        assertEquals(1, result.segments().size());
        assertEquals("test_table_OFFLINE_99_0", result.segments().get(0).segmentName());
        // 2 records per file × 2 files
        assertEquals(4, result.segments().get(0).outputRecordCount());
        verify(writer, times(1)).write(anyString(), any(Path.class));
        // Reader is called exactly once (single pass)
        verify(reader, times(1)).read();
    }

    // ── partitioned table config ──────────────────────────────────────────────

    @Test
    void shouldGenerateTwoSegmentsForPartitionedTableConfig() throws Exception {
        // With 2 partitions all records land in 0 or 1; use a real modulo function
        // so we can predict routing: "a".hashCode() % 2 and "b".hashCode() % 2 differ
        PartitionFunction modulo = (value, numPartitions) ->
                Math.abs(value.hashCode()) % numPartitions;

        // Write 4 records with IDs designed to hit both partitions
        Path dataFile = tempDir.resolve("data.json");
        Files.writeString(dataFile,
                "{\"id\":\"a\",\"name\":\"Alice\",\"value\":1,\"ts\":1700000000000}\n" +
                "{\"id\":\"b\",\"name\":\"Bob\",\"value\":2,\"ts\":1700000000000}\n" +
                "{\"id\":\"c\",\"name\":\"Carol\",\"value\":3,\"ts\":1700000000000}\n" +
                "{\"id\":\"d\",\"name\":\"Dave\",\"value\":4,\"ts\":1700000000000}\n",
                StandardCharsets.UTF_8);

        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "42", "json", schema, partitionedTableConfig, modulo);

        GenerationResult result = segmenter.generateSegment();

        // Must produce exactly 2 segments (one per partition)
        assertEquals(2, result.segments().size());

        long totalOutputRecords = result.segments().stream()
                .mapToLong(SegmentInfo::outputRecordCount)
                .sum();
        // All 4 input records should be present across the two segments
        assertEquals(4, totalOutputRecords);

        // Reader was invoked once → single-pass guarantee
        verify(reader, times(1)).read();
        // Writer called twice, once per segment
        verify(writer, times(2)).write(anyString(), any(Path.class));
    }

    @Test
    void shouldGenerateSegmentsEvenWhenOnePartitionIsEmpty() throws Exception {
        // Force all records into partition 0 by always returning 0
        PartitionFunction alwaysZero = (value, numPartitions) -> 0;

        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "55", "json", schema, partitionedTableConfig, alwaysZero);

        GenerationResult result = segmenter.generateSegment();

        assertEquals(2, result.segments().size());

        long totalRecords = result.segments().stream()
                .mapToLong(SegmentInfo::outputRecordCount).sum();
        assertEquals(2, totalRecords); // 2 records, all in partition 0; partition 1 = empty segment

        verify(reader, times(1)).read();
        verify(writer, times(2)).write(anyString(), any(Path.class));
    }

    // ── local artifact retention (URI/METADATA disk savings) ──────────────────

    @Test
    void shouldFreeTarAndMetadataWhenRetentionFullyDisabled() throws Exception {
        // URI-mode semantics: the uploader pushes by remote URI and reads neither local file.
        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "77", "json", schema, tableConfig, null)
                .setLocalArtifactRetention(false, false);

        GenerationResult result = segmenter.generateSegment();

        assertEquals(1, result.segments().size());
        SegmentInfo seg = result.segments().get(0);

        // Segment was still persisted to deep storage...
        assertTrue(seg.remoteURI().startsWith("oss://bucket/segments/"));
        verify(writer, times(1)).write(anyString(), any(Path.class));

        // ...but the local tar and metadata were freed immediately after the write.
        assertNull(seg.localPath());
        assertNull(seg.metadataPath());
    }

    @Test
    void shouldFreeTarButKeepMetadataForMetadataMode() throws Exception {
        // METADATA-mode semantics: the uploader needs the small metadata tar but not the segment tar.
        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenAnswer(inv -> "oss://bucket/segments/" + inv.getArgument(0));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "78", "json", schema, tableConfig, null)
                .setLocalArtifactRetention(false, true);

        GenerationResult result = segmenter.generateSegment();

        SegmentInfo seg = result.segments().get(0);
        assertNull(seg.localPath());
        assertNotNull(seg.metadataPath());
        assertTrue(Files.exists(seg.metadataPath()));
    }

    // ── error propagation ─────────────────────────────────────────────────────

    @Test
    void shouldPropagateReaderException() throws Exception {
        when(reader.read()).thenThrow(new IOException("OSS connection failed"));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "key", "json", schema, tableConfig, null);

        assertThrows(IOException.class, segmenter::generateSegment);
    }

    @Test
    void shouldPropagateWriterException() throws Exception {
        Path dataFile = createTestDataFile();
        when(reader.read()).thenReturn(List.of(dataFile));
        when(writer.write(anyString(), any(Path.class)))
                .thenThrow(new IOException("OSS upload failed"));

        PinotSegmenter segmenter = new PinotSegmenter(
                reader, writer, "key", "json", schema, tableConfig, null);

        assertThrows(IOException.class, segmenter::generateSegment);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

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

