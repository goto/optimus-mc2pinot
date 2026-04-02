package com.gojek.mc2pinot.io.s3;

import com.gojek.mc2pinot.io.Reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class S3Reader implements Reader {

    @Override
    public List<Path> read() throws IOException {
        throw new UnsupportedOperationException("S3 reader is not yet implemented");
    }

    @Override
    public void close() throws IOException {
    }
}

