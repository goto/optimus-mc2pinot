package com.gojek.mc2pinot.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MaxcomputeConfigTest {

    @Test
    void shouldParseValidServiceAccount() {
        Map<String, String> env = new HashMap<>();
        env.put("MC__SERVICE_ACCOUNT", """
                {"project_name":"my_project","access_id":"id123","access_key":"key456","endpoint":"http://mc.endpoint"}""");
        env.put("MC__QUERY_FILE_PATH", "data/in/query.sql");

        MaxcomputeConfig config = new MaxcomputeConfig(env);

        assertEquals("my_project", config.getProjectName());
        assertEquals("id123", config.getAccessId());
        assertEquals("key456", config.getAccessKey());
        assertEquals("http://mc.endpoint", config.getEndpoint());
        assertEquals("data/in/query.sql", config.getQueryFilePath());
    }

    @Test
    void shouldThrowWhenServiceAccountMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("MC__QUERY_FILE_PATH", "data/in/query.sql");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("MC__SERVICE_ACCOUNT"));
    }

    @Test
    void shouldThrowWhenQueryFilePathMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("MC__SERVICE_ACCOUNT", """
                {"project_name":"p","access_id":"i","access_key":"k","endpoint":"e"}""");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("MC__QUERY_FILE_PATH"));
    }

    @Test
    void shouldThrowWhenJsonFieldMissing() {
        Map<String, String> env = new HashMap<>();
        env.put("MC__SERVICE_ACCOUNT", """
                {"project_name":"p","access_id":"i","access_key":"k"}""");
        env.put("MC__QUERY_FILE_PATH", "data/in/query.sql");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new MaxcomputeConfig(env));
        assertTrue(ex.getMessage().contains("endpoint"));
    }
}

