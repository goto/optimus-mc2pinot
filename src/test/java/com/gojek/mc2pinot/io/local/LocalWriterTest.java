package com.gojek.mc2pinot.io.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LocalWriterTest {

    @TempDir
    Path tempDir;

    @TempDir
    Path sourceDir;

    @Test
    void shouldCopyFileToDestinationDirectory() throws IOException {
        Path sourceFile = sourceDir.resolve("segment.tar.gz");
        Files.writeString(sourceFile, "segment-data");

        LocalWriter writer = new LocalWriter(tempDir.toUri().toString());
        String resultURI = writer.write("segment.tar.gz", sourceFile);

        Path expectedTarget = tempDir.resolve("segment.tar.gz");
        assertTrue(Files.exists(expectedTarget));
        assertEquals("segment-data", Files.readString(expectedTarget));
        assertTrue(resultURI.endsWith("segment.tar.gz"));
    }

    @Test
    void shouldCreateDestinationDirectoryIfAbsent() throws IOException {
        Path sourceFile = sourceDir.resolve("seg.tar.gz");
        Files.writeString(sourceFile, "data");

        LocalWriter writer = new LocalWriter(tempDir.resolve("output").toUri().toString());
        writer.write("seg.tar.gz", sourceFile);

        assertTrue(Files.isDirectory(tempDir.resolve("output")));
    }

    @Test
    void shouldOverwriteExistingFile() throws IOException {
        Files.writeString(tempDir.resolve("seg.tar.gz"), "old-data");

        Path sourceFile = sourceDir.resolve("seg.tar.gz");
        Files.writeString(sourceFile, "new-data");

        LocalWriter writer = new LocalWriter(tempDir.toUri().toString());
        writer.write("seg.tar.gz", sourceFile);

        assertEquals("new-data", Files.readString(tempDir.resolve("seg.tar.gz")));
    }
}

