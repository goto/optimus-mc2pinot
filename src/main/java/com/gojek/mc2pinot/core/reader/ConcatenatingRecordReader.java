package com.gojek.mc2pinot.core.reader;

import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;

import java.io.IOException;
import java.util.List;
import java.util.Set;


/**
 * Wraps multiple {@link RecordReader}s and exposes them as a single sequential
 * stream. Used when one partition spans several split files (e.g. multiple
 * input avro files each producing a part-N.avro).
 */
public class ConcatenatingRecordReader implements RecordReader {

    private final List<RecordReader> readers;
    private int current = 0;

    public ConcatenatingRecordReader(List<RecordReader> readers) {
        this.readers = readers;
    }

    @Override
    public void init(java.io.File dataFile,
                     Set<String> fieldsToRead,
                     org.apache.pinot.spi.data.readers.RecordReaderConfig cfg) {
        // already initialised in factory
    }

    @Override
    public boolean hasNext() {
        while (current < readers.size()) {
            if (readers.get(current).hasNext()) return true;
            current++;
        }
        return false;
    }

    @Override
    public GenericRow next(GenericRow reuse) throws IOException {
        return readers.get(current).next(reuse);
    }

    @Override
    public void rewind() throws IOException {
        current = 0;
        for (RecordReader r : readers) r.rewind();
    }

    @Override
    public void close() throws IOException {
        for (RecordReader r : readers) r.close();
    }
}
