package com.gojek.mc2pinot.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Reader extends Closeable {

    List<Path> read() throws IOException;
}

