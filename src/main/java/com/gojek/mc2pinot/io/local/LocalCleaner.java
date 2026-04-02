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
    public void clean(String destinationURI) throws IOException {
        URI uri = URI.create(destinationURI);
        String pathStr = uri.getScheme() != null ? uri.getPath() : destinationURI;
        Path dir = Paths.get(pathStr);

        if (!Files.exists(dir)) {
            LOG.info("fs(local): destination does not exist, nothing to clean: " + dir);
            return;
        }

        LOG.info("fs(local): clean destination " + dir);
        deleteRecursively(dir);
        Files.createDirectories(dir);
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

