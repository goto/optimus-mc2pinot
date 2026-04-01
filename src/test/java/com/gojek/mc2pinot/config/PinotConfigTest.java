package com.gojek.mc2pinot.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PinotConfigTest {

    @Test
    void shouldParseValidConfig() {
        Map<String, String> env = new HashMap<>();
        env.put("PINOT__HOST", "http://localhost:9000");
        env.put("PINOT__SEGMENT_KEY", "1774310400000");
        env.put("PINOT__INPUT_FORMAT", "PARQUET");
        env.put("PINOT__SCHEMA_FILE_PATH", "/path/to/schema.json");
        env.put("PINOT__TABLE_CONFIG_FILE_PATH", "/path/to/tableConfig.json");

        PinotConfig config = new PinotConfig(env);

        assertEquals("http://localhost:9000", config.getHost());
        assertEquals("1774310400000", config.getSegmentKey());
        assertEquals("parquet", config.getInputFormat());
        assertEquals("/path/to/schema.json", config.getSchemaFilePath());
        assertEquals("/path/to/tableConfig.json", config.getTableConfigFilePath());
    }

    @Test
    void shouldLowercaseInputFormat() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__INPUT_FORMAT", "JSON");

        PinotConfig config = new PinotConfig(env);

        assertEquals("json", config.getInputFormat());
    }

    @Test
    void shouldThrowWhenHostMissing() {
        Map<String, String> env = buildValidEnv();
        env.remove("PINOT__HOST");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PinotConfig(env));
        assertTrue(ex.getMessage().contains("PINOT__HOST"));
    }

    @Test
    void shouldThrowWhenSegmentKeyMissing() {
        Map<String, String> env = buildValidEnv();
        env.remove("PINOT__SEGMENT_KEY");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PinotConfig(env));
        assertTrue(ex.getMessage().contains("PINOT__SEGMENT_KEY"));
    }

    @Test
    void shouldThrowWhenInputFormatMissing() {
        Map<String, String> env = buildValidEnv();
        env.remove("PINOT__INPUT_FORMAT");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PinotConfig(env));
        assertTrue(ex.getMessage().contains("PINOT__INPUT_FORMAT"));
    }

    private Map<String, String> buildValidEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("PINOT__HOST", "http://localhost:9000");
        env.put("PINOT__SEGMENT_KEY", "1774310400000");
        env.put("PINOT__INPUT_FORMAT", "json");
        env.put("PINOT__SCHEMA_FILE_PATH", "/path/to/schema.json");
        env.put("PINOT__TABLE_CONFIG_FILE_PATH", "/path/to/tableConfig.json");
        return env;
    }
}

