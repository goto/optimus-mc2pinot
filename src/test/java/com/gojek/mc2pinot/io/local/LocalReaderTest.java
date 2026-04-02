package com.gojek.mc2pinot.io.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LocalReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldListFilesFromDirectory() throws IOException {
        Files.writeString(tempDir.resolve("part-0.json"), "{\"id\":1}");
        Files.writeString(tempDir.resolve("part-1.json"), "{\"id\":2}");

        LocalReader reader = new LocalReader(tempDir.toUri().toString());
        List<Path> files = reader.read();

        assertEquals(2, files.size());
    }

    @Test
    void shouldReturnEmptyListWhenDirectoryIsEmpty() throws IOException {
        LocalReader reader = new LocalReader(tempDir.toUri().toString());
        List<Path> files = reader.read();

        assertTrue(files.isEmpty());
    }

    @Test
    void shouldReturnEmptyListWhenDirectoryDoesNotExist() throws IOException {
        LocalReader reader = new LocalReader(tempDir.resolve("nonexistent").toUri().toString());
        List<Path> files = reader.read();

        assertTrue(files.isEmpty());
    }

    @Test
    void shouldNotIncludeSubdirectories() throws IOException {
        Files.writeString(tempDir.resolve("file.json"), "{}");
        Files.createDirectory(tempDir.resolve("subdir"));

        LocalReader reader = new LocalReader(tempDir.toUri().toString());
        List<Path> files = reader.read();

        assertEquals(1, files.size());
        assertTrue(files.get(0).getFileName().toString().endsWith(".json"));
    }
}

