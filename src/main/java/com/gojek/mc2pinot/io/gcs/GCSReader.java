package com.gojek.mc2pinot.io.gcs;

import com.gojek.mc2pinot.io.Reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class GCSReader implements Reader {

    @Override
    public List<Path> read() throws IOException {
        throw new UnsupportedOperationException("GCS reader is not yet implemented");
    }

    @Override
    public void close() throws IOException {
    }
}

