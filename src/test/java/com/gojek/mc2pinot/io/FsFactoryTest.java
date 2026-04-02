package com.gojek.mc2pinot.io;

import com.gojek.mc2pinot.config.FsConfig;
import com.gojek.mc2pinot.io.local.LocalCleaner;
import com.gojek.mc2pinot.io.local.LocalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FsFactoryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCreateLocalComponentsForFileScheme() {
        Map<String, String> env = new HashMap<>();
        env.put("FS__DESTINATION_URI", tempDir.toUri().toString());

        FsConfig config = new FsConfig(env);
        FsFactory.FsComponents components = FsFactory.create(config);

        assertNotNull(components.writer());
        assertNotNull(components.cleaner());
        assertInstanceOf(LocalWriter.class, components.writer());
        assertInstanceOf(LocalCleaner.class, components.cleaner());
    }

    @Test
    void shouldThrowForS3Scheme() {
        Map<String, String> env = new HashMap<>();
        env.put("FS__DESTINATION_URI", "s3://my-bucket/path");

        FsConfig config = new FsConfig(env);

        assertThrows(UnsupportedOperationException.class, () -> FsFactory.create(config));
    }

    @Test
    void shouldThrowForGCSScheme() {
        Map<String, String> env = new HashMap<>();
        env.put("FS__DESTINATION_URI", "gs://my-bucket/path");

        FsConfig config = new FsConfig(env);

        assertThrows(UnsupportedOperationException.class, () -> FsFactory.create(config));
    }

    @Test
    void shouldThrowForUnknownScheme() {
        Map<String, String> env = new HashMap<>();
        env.put("FS__DESTINATION_URI", "ftp://my-server/path");

        FsConfig config = new FsConfig(env);

        assertThrows(IllegalArgumentException.class, () -> FsFactory.create(config));
    }

    @Test
    void shouldThrowWhenDestinationURIMissing() {
        Map<String, String> env = new HashMap<>();

        assertThrows(IllegalArgumentException.class, () -> new FsConfig(env));
    }
}

