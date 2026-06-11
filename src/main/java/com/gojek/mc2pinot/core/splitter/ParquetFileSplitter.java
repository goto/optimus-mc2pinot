package com.gojek.mc2pinot.core.splitter;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.core.partition.PartitionSpec;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.pinot.spi.data.Schema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetFileSplitter implements FileSplitter {

    private final PartitionSpec spec;
    private final Path splitDir;
    private final Schema pinotSchema;
    private final PartitionFunction partitionFunction;

    private final Map<Integer, ParquetWriter<GenericRecord>> writers = new HashMap<>();
    private final Map<Integer, Path> outputPaths = new HashMap<>();

    public ParquetFileSplitter(PartitionSpec spec, Path splitDir,
                        Schema pinotSchema, PartitionFunction partitionFunction) {
        this.spec = spec;
        this.splitDir = splitDir;
        this.pinotSchema = pinotSchema;
        this.partitionFunction = partitionFunction;
    }

    @Override
    public void split(Path inputFile) throws Exception {
        org.apache.hadoop.fs.Path hadoopPath =
                new org.apache.hadoop.fs.Path(inputFile.toUri());

        Configuration conf = new Configuration();
        conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);

        try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader =
                     AvroParquetReader.<GenericRecord>builder(hadoopPath).withConf(conf).build()) {

            org.apache.avro.Schema avroSchema = null;
            GenericRecord record;
            while ((record = reader.read()) != null) {
                if (avroSchema == null) {
                    avroSchema = record.getSchema();
                }
                int partId = computePartition(record);
                getOrCreateWriter(partId, avroSchema).write(record);
            }
        }
    }

    private int computePartition(GenericRecord record) {
        if (spec.column() == null) return 0;
        Object value = record.get(spec.column());
        return partitionFunction.partition(value != null ? value.toString() : "", spec.count());
    }

    private ParquetWriter<GenericRecord> getOrCreateWriter(
            int partId, org.apache.avro.Schema avroSchema) throws Exception {
        if (!writers.containsKey(partId)) {
            Path out = splitDir.resolve("part-" + partId + ".parquet");
            outputPaths.put(partId, out);
            org.apache.hadoop.fs.Path hadoopOut =
                    new org.apache.hadoop.fs.Path(out.toUri());
            // Use a small row-group size (16 MB instead of the 128 MB default) so that
            // each concurrent ParquetWriter only buffers a bounded amount of data in RAM.
            ParquetWriter<GenericRecord> w = AvroParquetWriter
                    .<GenericRecord>builder(hadoopOut)
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(16L * 1024 * 1024)
                    .build();
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
        for (ParquetWriter<GenericRecord> w : writers.values()) {
            try { w.close(); } catch (IOException e) { /* log and continue */ }
        }
    }
}
