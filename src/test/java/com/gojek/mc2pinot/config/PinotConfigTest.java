package com.gojek.mc2pinot.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PinotConfigTest {

    private static final String VALID_OSS_SERVICE_ACCOUNT =
            "{\"access_key_id\":\"akid\",\"access_key_secret\":\"aksecret\",\"endpoint\":\"https://oss.endpoint\",\"region\":\"ap-southeast-5\"}";

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
        assertNull(config.getDeepStorageURI());
        assertNull(config.getDeepStorageOssConfig());
        assertNull(config.getCustomHeadersPath());
    }

    @Test
    void shouldReturnCustomHeadersPath() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__CUSTOM_HEADERS_PATH", "/path/to/headers.json");

        PinotConfig config = new PinotConfig(env);

        assertEquals("/path/to/headers.json", config.getCustomHeadersPath());
    }

    @Test
    void shouldLowercaseInputFormat() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__INPUT_FORMAT", "JSON");

        PinotConfig config = new PinotConfig(env);

        assertEquals("json", config.getInputFormat());
    }

    @Test
    void shouldAcceptAbsentDeepStorageURI() {
        PinotConfig config = new PinotConfig(buildValidEnv());

        assertNull(config.getDeepStorageURI());
        assertNull(config.getDeepStorageOssConfig());
    }

    @Test
    void shouldAcceptLocalDeepStorageURI() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__DEEP_STORAGE_URI", "file:///tmp/deep-storage");

        PinotConfig config = new PinotConfig(env);

        assertEquals("file:///tmp/deep-storage", config.getDeepStorageURI());
        assertNull(config.getDeepStorageOssConfig());
    }

    @Test
    void shouldWireOSSConfigWhenDeepStorageURIIsOSS() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__DEEP_STORAGE_URI", "oss://my-bucket/deep-storage");
        env.put("PINOT__DEEP_STORAGE_OSS_SERVICE_ACCOUNT", VALID_OSS_SERVICE_ACCOUNT);

        PinotConfig config = new PinotConfig(env);

        assertEquals("oss://my-bucket/deep-storage", config.getDeepStorageURI());
        assertNotNull(config.getDeepStorageOssConfig());
        assertEquals("akid", config.getDeepStorageOssConfig().getAccessKeyId());
    }

    @Test
    void shouldDefaultSegmentPushDelayTo30Seconds() {
        PinotConfig config = new PinotConfig(buildValidEnv());

        assertEquals(30L, config.getSegmentPushDelayInSeconds());
    }

    @Test
    void shouldReadConfiguredSegmentPushDelay() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__SEGMENT_PUSH_DELAY_IN_SECONDS", "5");

        PinotConfig config = new PinotConfig(env);

        assertEquals(5L, config.getSegmentPushDelayInSeconds());
    }

    @Test
    void shouldAllowZeroSegmentPushDelay() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__SEGMENT_PUSH_DELAY_IN_SECONDS", "0");

        PinotConfig config = new PinotConfig(env);

        assertEquals(0L, config.getSegmentPushDelayInSeconds());
    }

    @Test
    void shouldThrowWhenSegmentPushDelayIsNotNumeric() {
        Map<String, String> env = buildValidEnv();
        env.put("PINOT__SEGMENT_PUSH_DELAY_IN_SECONDS", "abc");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new PinotConfig(env));
        assertTrue(ex.getMessage().contains("PINOT__SEGMENT_PUSH_DELAY_IN_SECONDS"));
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
