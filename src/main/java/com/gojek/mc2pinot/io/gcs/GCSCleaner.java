package com.gojek.mc2pinot.io.gcs;

import com.gojek.mc2pinot.io.Cleaner;

import java.io.IOException;

public class GCSCleaner implements Cleaner {

    @Override
    public void clean(String uri) throws IOException {
        throw new UnsupportedOperationException("GCS cleaner is not yet implemented");
    }
}
