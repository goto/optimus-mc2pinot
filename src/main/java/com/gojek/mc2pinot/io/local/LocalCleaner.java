package com.gojek.mc2pinot.io.local;

import com.gojek.mc2pinot.io.Cleaner;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class LocalCleaner implements Cleaner {

    private static final Logger LOG = Logger.getLogger(LocalCleaner.class.getName());

    @Override
    public void clean(String uri) throws IOException {
        URI parsed = URI.create(uri);
        String pathStr = parsed.getScheme() != null ? parsed.getPath() : uri;
        Path path = Paths.get(pathStr);

        if (Files.isDirectory(path)) {
            LOG.info("transient(local): clean directory " + path);
            deleteRecursively(path);
            Files.createDirectories(path);
        } else if (Files.isRegularFile(path)) {
            LOG.info("transient(local): delete segment " + path);
            Files.deleteIfExists(path);
        } else {
            LOG.info("transient(local): path does not exist, nothing to clean: " + path);
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var entries = Files.list(path)) {
                for (Path entry : entries.toList()) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
