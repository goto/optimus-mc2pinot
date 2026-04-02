package com.gojek.mc2pinot.io;

import java.io.IOException;

public interface Cleaner {

    void clean(String destinationURI) throws IOException;
}

