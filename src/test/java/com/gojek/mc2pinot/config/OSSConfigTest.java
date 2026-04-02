package com.gojek.mc2pinot.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OSSConfigTest {

    private static final String VALID_SERVICE_ACCOUNT = """
            {"access_key_id":"akid","access_key_secret":"aksecret","endpoint":"https://oss.endpoint","region":"ap-southeast-5"}""";

    @Test
    void shouldParseValidConfig() {
        Map<String, String> env = new HashMap<>();
        env.put("PINOT__DEEP_STORAGE_OSS_SERVICE_ACCOUNT", VALID_SERVICE_ACCOUNT);

        OSSConfig config = new OSSConfig(env);

        assertEquals("akid", config.getAccessKeyId());
        assertEquals("aksecret", config.getAccessKeySecret());
        assertEquals("https://oss.endpoint", config.getEndpoint());
        assertEquals("ap-southeast-5", config.getRegion());
    }

    @Test
    void shouldThrowWhenServiceAccountMissing() {
        Map<String, String> env = new HashMap<>();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new OSSConfig(env));
        assertTrue(ex.getMessage().contains("PINOT__DEEP_STORAGE_OSS_SERVICE_ACCOUNT"));
    }

    @Test
    void shouldThrowWhenAccessKeyIdMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("PINOT__DEEP_STORAGE_OSS_SERVICE_ACCOUNT",
                """
                {"access_key_secret":"s","endpoint":"e","region":"r"}""");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new OSSConfig(env));
        assertTrue(ex.getMessage().contains("access_key_id"));
    }

    @Test
    void shouldThrowWhenEndpointMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("PINOT__DEEP_STORAGE_OSS_SERVICE_ACCOUNT",
                """
                {"access_key_id":"i","access_key_secret":"s","region":"r"}""");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new OSSConfig(env));
        assertTrue(ex.getMessage().contains("endpoint"));
    }
}
