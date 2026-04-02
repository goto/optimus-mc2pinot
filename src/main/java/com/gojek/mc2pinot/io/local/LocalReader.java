package com.gojek.mc2pinot.io.local;

import com.gojek.mc2pinot.io.Reader;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class LocalReader implements Reader {

    private final Path sourceDir;

    public LocalReader(String destinationURI) {
        URI uri = URI.create(destinationURI);
        String path = uri.getScheme() != null ? uri.getPath() : destinationURI;
        this.sourceDir = Paths.get(path);
    }

    @Override
    public List<Path> read() throws IOException {
        if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
            return Collections.emptyList();
        }

        try (Stream<Path> stream = Files.list(sourceDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .toList();
        }
    }

    @Override
    public void close() throws IOException {
    }
}

