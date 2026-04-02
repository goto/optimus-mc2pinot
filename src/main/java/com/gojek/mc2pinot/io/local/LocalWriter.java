package com.gojek.mc2pinot.io.local;

import com.gojek.mc2pinot.io.Writer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class LocalWriter implements Writer {

    private final Path destinationDir;

    public LocalWriter(String destinationURI) {
        URI uri = URI.create(destinationURI);
        String path = uri.getScheme() != null ? uri.getPath() : destinationURI;
        this.destinationDir = Paths.get(path);
    }

    @Override
    public String write(String objectKey, Path localFile) throws IOException {
        Files.createDirectories(destinationDir);

        Path target = destinationDir.resolve(objectKey);
        Files.copy(localFile, target, StandardCopyOption.REPLACE_EXISTING);

        return target.toUri().toString();
    }
}

