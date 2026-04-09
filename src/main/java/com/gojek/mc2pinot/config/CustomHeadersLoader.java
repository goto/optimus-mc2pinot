package com.gojek.mc2pinot.config;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

public class CustomHeadersLoader {

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() {}.getType();

    private final String headersPath;

    public CustomHeadersLoader(String headersPath) {
        this.headersPath = headersPath;
    }

    public Map<String, String> load() throws IOException {
        if (headersPath == null || headersPath.isBlank()) {
            return Collections.emptyMap();
        }

        Path path = Path.of(headersPath).toAbsolutePath();
        String content = Files.readString(path, StandardCharsets.UTF_8);

        try {
            Map<String, String> headers = GSON.fromJson(content, MAP_TYPE);
            if (headers == null) {
                throw new IOException("Custom headers file is empty or null: " + path);
            }
            return headers;
        } catch (JsonSyntaxException e) {
            throw new IOException("Failed to parse custom headers JSON from: " + path, e);
        }
    }
}

