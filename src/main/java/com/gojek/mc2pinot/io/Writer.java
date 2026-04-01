package com.gojek.mc2pinot.io;

import java.io.IOException;
import java.nio.file.Path;

public interface Writer {

    String write(String objectKey, Path localFile) throws IOException;
}

