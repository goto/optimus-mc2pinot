package com.gojek.mc2pinot.io.gcs;

import com.gojek.mc2pinot.io.Writer;

import java.io.IOException;
import java.nio.file.Path;

public class GCSWriter implements Writer {

    @Override
    public String write(String objectKey, Path localFile) throws IOException {
        throw new UnsupportedOperationException("GCS writer is not yet implemented");
    }
}

