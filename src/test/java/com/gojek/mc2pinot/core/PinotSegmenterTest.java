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

    @Mock
    private PartitionFunction partitionFunction;

    @TempDir
    Path tempDir;

    private Schema schema;
    private TableConfig tableConfig;

    @BeforeEach
    void setUp() throws Exception {
        schema = loadSchema("/test-schema.json");
        tableConfig = loadTableConfig("/test-tableConfig.json");
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

