package com.gojek.mc2pinot.io.s3;

import com.gojek.mc2pinot.io.Cleaner;

import java.io.IOException;

public class S3Cleaner implements Cleaner {

    @Override
    public void clean(String uri) throws IOException {
        throw new UnsupportedOperationException("S3 cleaner is not yet implemented");
    }
}
