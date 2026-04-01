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
        env.put("OSS__SERVICE_ACCOUNT", VALID_SERVICE_ACCOUNT);
        env.put("OSS__DESTINATION_URI", "oss://bucket/path/to/data");
        env.put("OSS__ROLE_ARN", "acs:ram::123456:role/myrole");

        OSSConfig config = new OSSConfig(env);

        assertEquals("akid", config.getAccessKeyId());
        assertEquals("aksecret", config.getAccessKeySecret());
        assertEquals("https://oss.endpoint", config.getEndpoint());
        assertEquals("ap-southeast-5", config.getRegion());
        assertEquals("oss://bucket/path/to/data", config.getDestinationURI());
        assertEquals("acs:ram::123456:role/myrole", config.getRoleArn());
    }

    @Test
    void shouldAppendSegmentsSuffix() {
        Map<String, String> env = new HashMap<>();
        env.put("OSS__SERVICE_ACCOUNT", VALID_SERVICE_ACCOUNT);
        env.put("OSS__DESTINATION_URI", "oss://bucket/path/to/data");
        env.put("OSS__ROLE_ARN", "acs:ram::123456:role/myrole");

        OSSConfig config = new OSSConfig(env);

        assertEquals("oss://bucket/path/to/data/segments", config.getSegmentOutputURI());
    }

    @Test
    void shouldAppendSegmentsSuffixWithTrailingSlash() {
        Map<String, String> env = new HashMap<>();
        env.put("OSS__SERVICE_ACCOUNT", VALID_SERVICE_ACCOUNT);
        env.put("OSS__DESTINATION_URI", "oss://bucket/path/to/data/");
        env.put("OSS__ROLE_ARN", "acs:ram::123456:role/myrole");

        OSSConfig config = new OSSConfig(env);

        assertEquals("oss://bucket/path/to/data/segments", config.getSegmentOutputURI());
    }

    @Test
    void shouldThrowWhenServiceAccountMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("OSS__DESTINATION_URI", "oss://bucket/path");
        env.put("OSS__ROLE_ARN", "acs:ram::123456:role/myrole");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new OSSConfig(env));
        assertTrue(ex.getMessage().contains("OSS__SERVICE_ACCOUNT"));
    }

    @Test
    void shouldThrowWhenDestinationURIMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("OSS__SERVICE_ACCOUNT", VALID_SERVICE_ACCOUNT);
        env.put("OSS__ROLE_ARN", "acs:ram::123456:role/myrole");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new OSSConfig(env));
        assertTrue(ex.getMessage().contains("OSS__DESTINATION_URI"));
    }

    @Test
    void shouldThrowWhenRoleArnMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("OSS__SERVICE_ACCOUNT", VALID_SERVICE_ACCOUNT);
        env.put("OSS__DESTINATION_URI", "oss://bucket/path");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new OSSConfig(env));
        assertTrue(ex.getMessage().contains("OSS__ROLE_ARN"));
    }
}

