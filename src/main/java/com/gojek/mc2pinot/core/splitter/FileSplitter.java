package com.gojek.mc2pinot.core.splitter;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Reads input files of a specific format, routes each row to the right
 * per-partition output file, and exposes the result as a map of paths.
 */
public interface FileSplitter extends Closeable {
    void split(Path inputFile) throws Exception;
    Map<Integer, List<Path>> splitFiles();
}