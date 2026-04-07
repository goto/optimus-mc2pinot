package com.gojek.mc2pinot.config;

import com.gojek.mc2pinot.metrics.SegmentPayloadContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PayloadTemplateRendererTest {

    @TempDir
    Path tempDir;

    private final SegmentPayloadContext ctx = new SegmentPayloadContext(
            100L, 204800L, "my_table_OFFLINE_12345_0", 50L, 102400L);

    @Test
    void shouldReturnFallbackWhenTemplatePathIsNull() throws IOException {
        PayloadTemplateRenderer renderer = new PayloadTemplateRenderer(null);
        assertEquals("{}", renderer.render(ctx));
    }

    @Test
    void shouldReturnFallbackWhenTemplatePathIsBlank() throws IOException {
        PayloadTemplateRenderer renderer = new PayloadTemplateRenderer("   ");
        assertEquals("{}", renderer.render(ctx));
    }

    @Test
    void shouldRenderTemplateWithAllMetricFields() throws IOException {
        Path templateFile = tempDir.resolve("payload.jte");
        String template = """
                @param com.gojek.mc2pinot.metrics.SegmentPayloadContext ctx
                {"input_count":${ctx.inputRecordCount()},"input_size":${ctx.inputRecordSize()},"segment":"${ctx.segmentName()}","output_count":${ctx.outputRecordCount()},"output_size":${ctx.outputRecordSize()}}""";
        Files.writeString(templateFile, template, StandardCharsets.UTF_8);

        PayloadTemplateRenderer renderer = new PayloadTemplateRenderer(templateFile.toString());
        String result = renderer.render(ctx);

        assertTrue(result.contains("\"input_count\":100"));
        assertTrue(result.contains("\"input_size\":204800"));
        assertTrue(result.contains("\"segment\":\"my_table_OFFLINE_12345_0\""));
        assertTrue(result.contains("\"output_count\":50"));
        assertTrue(result.contains("\"output_size\":102400"));
    }

    @Test
    void shouldThrowWhenTemplateFileDoesNotExist() {
        PayloadTemplateRenderer renderer = new PayloadTemplateRenderer(
                tempDir.resolve("nonexistent.jte").toString());

        assertThrows(Exception.class, () -> renderer.render(ctx));
    }
}

