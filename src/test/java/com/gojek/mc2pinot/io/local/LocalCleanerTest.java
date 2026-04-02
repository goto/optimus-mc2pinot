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

        LocalCleaner cleaner = new LocalCleaner();
        cleaner.clean(tempDir.toUri().toString());

        assertTrue(Files.isDirectory(tempDir));
        assertEquals(0, Files.list(tempDir).count());
    }

    @Test
    void shouldBeNoOpWhenDirectoryDoesNotExist() {
        Path nonExistent = tempDir.resolve("ghost");

        LocalCleaner cleaner = new LocalCleaner();
        assertDoesNotThrow(() -> cleaner.clean(nonExistent.toUri().toString()));
    }

    @Test
    void shouldDeleteNestedContentsRecursively() throws IOException {
        Path sub = tempDir.resolve("sub");
        Files.createDirectories(sub);
        Files.writeString(sub.resolve("nested.txt"), "data");
        Files.writeString(tempDir.resolve("top.txt"), "data");

        LocalCleaner cleaner = new LocalCleaner();
        cleaner.clean(tempDir.toUri().toString());

        assertTrue(Files.isDirectory(tempDir));
        assertEquals(0, Files.list(tempDir).count());
    }
}

