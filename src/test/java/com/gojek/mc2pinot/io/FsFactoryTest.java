package com.gojek.mc2pinot.io;

import com.gojek.mc2pinot.config.PinotConfig;
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
    void shouldCreateLocalComponentsWhenDeepStorageURIAbsent() {
        PinotConfig config = buildPinotConfig(null);
        String segmentFolderURI = tempDir.toUri().toString();

        FsFactory.FsComponents components = FsFactory.create(config, segmentFolderURI);

        assertNotNull(components.writer());
        assertNotNull(components.cleaner());
        assertInstanceOf(LocalWriter.class, components.writer());
        assertInstanceOf(LocalCleaner.class, components.cleaner());
    }

    @Test
    void shouldCreateLocalComponentsForFileScheme() {
        PinotConfig config = buildPinotConfig("file:///tmp/deep-storage");
        String segmentFolderURI = tempDir.toUri().toString();

        FsFactory.FsComponents components = FsFactory.create(config, segmentFolderURI);

        assertNotNull(components.writer());
        assertNotNull(components.cleaner());
        assertInstanceOf(LocalWriter.class, components.writer());
        assertInstanceOf(LocalCleaner.class, components.cleaner());
    }

    @Test
    void shouldThrowForS3Scheme() {
        PinotConfig config = buildPinotConfig("s3://my-bucket/deep-storage");

        assertThrows(UnsupportedOperationException.class,
                () -> FsFactory.create(config, "s3://my-bucket/deep-storage/my_table/segments_123"));
    }

    @Test
    void shouldThrowForGCSScheme() {
        PinotConfig config = buildPinotConfig("gs://my-bucket/deep-storage");

        assertThrows(UnsupportedOperationException.class,
                () -> FsFactory.create(config, "gs://my-bucket/deep-storage/my_table/segments_123"));
    }

    @Test
    void shouldThrowForUnknownScheme() {
        PinotConfig config = buildPinotConfig("ftp://my-server/deep-storage");

        assertThrows(IllegalArgumentException.class,
                () -> FsFactory.create(config, "ftp://my-server/deep-storage/my_table/segments_123"));
    }

    private PinotConfig buildPinotConfig(String deepStorageURI) {
        Map<String, String> env = new HashMap<>();
        env.put("PINOT__HOST", "http://localhost:9000");
        env.put("PINOT__SEGMENT_KEY", "123");
        env.put("PINOT__INPUT_FORMAT", "json");
        env.put("PINOT__SCHEMA_FILE_PATH", "/path/to/schema.json");
        env.put("PINOT__TABLE_CONFIG_FILE_PATH", "/path/to/tableConfig.json");
        if (deepStorageURI != null) {
            env.put("PINOT__DEEP_STORAGE_URI", deepStorageURI);
        }
        return new PinotConfig(env);
    }
}
