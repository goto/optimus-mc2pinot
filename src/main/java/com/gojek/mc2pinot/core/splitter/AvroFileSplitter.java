package com.gojek.mc2pinot.core.splitter;

import com.gojek.mc2pinot.core.partition.PartitionSpec;
import com.gojek.mc2pinot.core.partition.SegmentAssigner;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.Schema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroFileSplitter implements FileSplitter {

    private final PartitionSpec spec;
    private final Path splitDir;
    private final Schema pinotSchema;
    private final SegmentAssigner assigner;

    private final Map<Integer, DataFileWriter<GenericRecord>> writers = new HashMap<>();
    private final Map<Integer, Path> outputPaths = new HashMap<>();

    public AvroFileSplitter(PartitionSpec spec, Path splitDir,
                     Schema pinotSchema, SegmentAssigner assigner) {
        this.spec = spec;
        this.splitDir = splitDir;
        this.pinotSchema = pinotSchema;
        this.assigner = assigner;
    }

    @Override
    public void split(Path inputFile) throws Exception {
        try (DataFileReader<GenericRecord> fileReader =
                     new DataFileReader<>(inputFile.toFile(), new GenericDatumReader<>())) {

            org.apache.avro.Schema avroSchema = fileReader.getSchema();

            for (GenericRecord record : fileReader) {
                int partId = computePartition(record);
                DataFileWriter<GenericRecord> w = getOrCreateWriter(partId, avroSchema);
                w.append(record);
            }
        }
    }

    private int computePartition(GenericRecord record) {
        if (spec.column() == null) return 0;
        Object value = record.get(spec.column());
        return assigner.assign(value != null ? value.toString() : "");
    }

    private DataFileWriter<GenericRecord> getOrCreateWriter(
            int partId, org.apache.avro.Schema avroSchema) throws Exception {
        if (!writers.containsKey(partId)) {
            Path out = splitDir.resolve("part-" + partId + ".avro");
            outputPaths.put(partId, out);
            DataFileWriter<GenericRecord> w =
                    new DataFileWriter<>(new GenericDatumWriter<>(avroSchema));
            w.create(avroSchema, out.toFile());
            writers.put(partId, w);
        }
        return writers.get(partId);
    }

    @Override
    public Map<Integer, List<Path>> splitFiles() {
        Map<Integer, List<Path>> result = new HashMap<>();
        for (Map.Entry<Integer, Path> e : outputPaths.entrySet()) {
            result.put(e.getKey(), Collections.singletonList(e.getValue()));
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        for (DataFileWriter<GenericRecord> w : writers.values()) {
            w.close();
        }
    }
}
