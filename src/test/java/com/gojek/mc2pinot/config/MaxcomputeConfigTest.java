package com.gojek.mc2pinot.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MaxcomputeConfigTest {

    private static final String VALID_MC_SA = """
            {"project_name":"my_project","access_id":"id123","access_key":"key456","endpoint":"http://mc.endpoint"}""";

    private static final String VALID_OSS_SA = """
            {"access_key_id":"akid","access_key_secret":"aksecret","endpoint":"https://oss.endpoint","region":"ap-southeast-5"}""";

    private Map<String, String> baseEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("MC__SERVICE_ACCOUNT", VALID_MC_SA);
        env.put("MC__QUERY_FILE_PATH", "data/in/query.sql");
        env.put("MC__OSS_SERVICE_ACCOUNT", VALID_OSS_SA);
        env.put("MC__OSS_DESTINATION_URI", "oss://bucket/staging");
        env.put("MC__OSS_ROLE_ARN", "acs:ram::123:role/mc");
        return env;
    }

    @Test
    void shouldParseValidConfig() {
        MaxcomputeConfig config = new MaxcomputeConfig(baseEnv());

        assertEquals("my_project", config.getProjectName());
        assertEquals("id123", config.getAccessId());
        assertEquals("key456", config.getAccessKey());
        assertEquals("http://mc.endpoint", config.getEndpoint());
        assertEquals("data/in/query.sql", config.getQueryFilePath());
        assertEquals("oss://bucket/staging", config.getOssDestinationURI());
        assertEquals("acs:ram::123:role/mc", config.getOssRoleArn());
        assertEquals("akid", config.getOssAccessKeyId());
        assertEquals("aksecret", config.getOssAccessKeySecret());
        assertEquals("https://oss.endpoint", config.getOssEndpoint());
        assertEquals("ap-southeast-5", config.getOssRegion());
    }

    @Test
    void shouldThrowWhenServiceAccountMissing() {
        Map<String, String> env = baseEnv();
        env.remove("MC__SERVICE_ACCOUNT");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("MC__SERVICE_ACCOUNT"));
    }

    @Test
    void shouldThrowWhenQueryFilePathMissing() {
        Map<String, String> env = baseEnv();
        env.remove("MC__QUERY_FILE_PATH");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("MC__QUERY_FILE_PATH"));
    }

    @Test
    void shouldThrowWhenOssDestinationURIMissing() {
        Map<String, String> env = baseEnv();
        env.remove("MC__OSS_DESTINATION_URI");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("MC__OSS_DESTINATION_URI"));
    }

    @Test
    void shouldThrowWhenOssRoleArnMissing() {
        Map<String, String> env = baseEnv();
        env.remove("MC__OSS_ROLE_ARN");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("MC__OSS_ROLE_ARN"));
    }

    @Test
    void shouldThrowWhenOssServiceAccountMissing() {
        Map<String, String> env = baseEnv();
        env.remove("MC__OSS_SERVICE_ACCOUNT");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("MC__OSS_SERVICE_ACCOUNT"));
    }

    @Test
    void shouldThrowWhenMcJsonFieldMissing() {
        Map<String, String> env = baseEnv();
        env.put("MC__SERVICE_ACCOUNT",
                """
                {"project_name":"p","access_id":"i","access_key":"k"}""");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("endpoint"));
    }
}
