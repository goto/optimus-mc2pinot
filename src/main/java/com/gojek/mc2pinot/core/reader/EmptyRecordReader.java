package com.gojek.mc2pinot.core.reader;

import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;

import java.util.Set;

public class EmptyRecordReader implements RecordReader {
    @Override
    public void init(java.io.File f, Set<String> fields,
                     org.apache.pinot.spi.data.readers.RecordReaderConfig cfg) {}
    @Override public boolean hasNext() { return false; }
    @Override public GenericRow next(GenericRow reuse) { return reuse; }
    @Override public void rewind() {}
    @Override public void close() {}
}
