package com.gojek.mc2pinot.io.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LocalCleanerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldDeleteDirectoryContentsAndRecreateIt() throws IOException {
        Files.writeString(tempDir.resolve("file1.txt"), "data");
        Files.writeString(tempDir.resolve("file2.txt"), "data");

        new LocalCleaner().clean(tempDir.toUri().toString());

        assertTrue(Files.isDirectory(tempDir));
        assertEquals(0, Files.list(tempDir).count());
    }

    @Test
    void shouldBeNoOpWhenPathDoesNotExist() {
        Path nonExistent = tempDir.resolve("ghost");

        assertDoesNotThrow(() -> new LocalCleaner().clean(nonExistent.toUri().toString()));
    }

    @Test
    void shouldDeleteNestedContentsRecursively() throws IOException {
        Path sub = tempDir.resolve("sub");
        Files.createDirectories(sub);
        Files.writeString(sub.resolve("nested.txt"), "data");
        Files.writeString(tempDir.resolve("top.txt"), "data");

        new LocalCleaner().clean(tempDir.toUri().toString());

        assertTrue(Files.isDirectory(tempDir));
        assertEquals(0, Files.list(tempDir).count());
    }

    @Test
    void shouldDeleteSingleFileWhenUriPointsToFile() throws IOException {
        Path file = Files.writeString(tempDir.resolve("seg_0.tar.gz"), "data");

        new LocalCleaner().clean(file.toUri().toString());

        assertFalse(Files.exists(file));
    }

    @Test
    void shouldBeNoOpWhenFileDoesNotExist() {
        Path nonExistent = tempDir.resolve("ghost.tar.gz");

        assertDoesNotThrow(() -> new LocalCleaner().clean(nonExistent.toUri().toString()));
    }

    @Test
    void shouldNotDeleteSiblingFilesOnSingleFileClean() throws IOException {
        Path target = Files.writeString(tempDir.resolve("seg_0.tar.gz"), "data");
        Path sibling = Files.writeString(tempDir.resolve("seg_1.tar.gz"), "data");

        new LocalCleaner().clean(target.toUri().toString());

        assertFalse(Files.exists(target));
        assertTrue(Files.exists(sibling));
    }
}
