package com.gojek.mc2pinot.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CustomHeadersLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldReturnEmptyMapWhenPathIsNull() throws IOException {
        Map<String, String> headers = new CustomHeadersLoader(null).load();
        assertTrue(headers.isEmpty());
    }

    @Test
    void shouldReturnEmptyMapWhenPathIsBlank() throws IOException {
        Map<String, String> headers = new CustomHeadersLoader("   ").load();
        assertTrue(headers.isEmpty());
    }

    @Test
    void shouldLoadHeadersFromValidJsonFile() throws IOException {
        Path file = tempDir.resolve("headers.json");
        Files.writeString(file, "{\"Authorization\":\"Bearer token\",\"X-Custom\":\"value\"}", StandardCharsets.UTF_8);

        Map<String, String> headers = new CustomHeadersLoader(file.toString()).load();

        assertEquals(2, headers.size());
        assertEquals("Bearer token", headers.get("Authorization"));
        assertEquals("value", headers.get("X-Custom"));
    }

    @Test
    void shouldThrowOnMalformedJson() {
        Path file = tempDir.resolve("headers.json");
        assertDoesNotThrow(() -> Files.writeString(file, "not-valid-json", StandardCharsets.UTF_8));

        assertThrows(IOException.class, () -> new CustomHeadersLoader(file.toString()).load());
    }

    @Test
    void shouldThrowOnNullJsonContent() {
        Path file = tempDir.resolve("headers.json");
        assertDoesNotThrow(() -> Files.writeString(file, "null", StandardCharsets.UTF_8));

        assertThrows(IOException.class, () -> new CustomHeadersLoader(file.toString()).load());
    }

    @Test
    void shouldThrowWhenFileDoesNotExist() {
        String nonExistentPath = tempDir.resolve("missing.json").toString();
        assertThrows(IOException.class, () -> new CustomHeadersLoader(nonExistentPath).load());
    }
}

