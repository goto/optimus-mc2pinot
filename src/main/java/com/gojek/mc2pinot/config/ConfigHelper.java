package com.gojek.mc2pinot.config;

import com.google.gson.JsonObject;

import java.util.Map;

public class ConfigHelper {

    public static String requireNonEmpty(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: " + key);
        }
        return value;
    }

    public static String optionalWithDefault(Map<String, String> env, String key, String defaultValue) {
        String value = env.get(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    public static String requireJsonField(JsonObject json, String key, String field) {
        if (!json.has(field) || json.get(field).getAsString().isBlank()) {
            throw new IllegalArgumentException("Missing required field in " + key + ": " + field);
        }
        return json.get(field).getAsString();
    }
}

