package com.gojek.mc2pinot.io.s3;

import com.gojek.mc2pinot.io.Writer;

import java.io.IOException;
import java.nio.file.Path;

public class S3Writer implements Writer {

    @Override
    public String write(String objectKey, Path localFile) throws IOException {
        throw new UnsupportedOperationException("S3 writer is not yet implemented");
    }
}

