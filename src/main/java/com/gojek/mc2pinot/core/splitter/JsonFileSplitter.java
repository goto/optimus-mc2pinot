package com.gojek.mc2pinot.core.splitter;

import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.core.partition.PartitionSpec;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class JsonFileSplitter implements FileSplitter {

    private final PartitionSpec spec;
    private final Path splitDir;
    private final Schema pinotSchema;
    private final PartitionFunction partitionFunction;

    private final Map<Integer, BufferedWriter> writers = new HashMap<>();
    private final Map<Integer, Path> outputPaths = new HashMap<>();

    public JsonFileSplitter(PartitionSpec spec, Path splitDir,
                     Schema pinotSchema, PartitionFunction partitionFunction) {
        this.spec = spec;
        this.splitDir = splitDir;
        this.pinotSchema = pinotSchema;
        this.partitionFunction = partitionFunction;
    }

    @Override
    public void split(Path inputFile) throws Exception {
        // Use Pinot's JSONRecordReader to parse each line/record uniformly
        Set<String> fields = new HashSet<>(pinotSchema.getPhysicalColumnNames());
        try (JSONRecordReader rr = new JSONRecordReader()) {
            rr.init(inputFile.toFile(), fields, null);
            GenericRow row = new GenericRow();
            while (rr.hasNext()) {
                row.clear();
                rr.next(row);
                int partId = computePartition(row);
                // serialize back to JSON line for the split file
                BufferedWriter w = getOrCreateWriter(partId);
                w.write(rowToJson(row));
                w.newLine();
            }
        }
    }

    private int computePartition(GenericRow row) {
        if (spec.column() == null) return 0;
        Object value = row.getValue(spec.column());
        return spec.segmentOf(value != null ? value.toString() : "", partitionFunction);
    }

    private BufferedWriter getOrCreateWriter(int partId) throws IOException {
        if (!writers.containsKey(partId)) {
            Path out = splitDir.resolve("part-" + partId + ".json");
            outputPaths.put(partId, out);
            writers.put(partId,
                    Files.newBufferedWriter(out));
        }
        return writers.get(partId);
    }

    /** Converts a GenericRow to a flat JSON object string. */
    private String rowToJson(GenericRow row) throws IOException {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (String field : row.getFieldNames()) {
            if (!first) sb.append(",");
            first = false;
            Object val = row.getValue(field);
            sb.append("\"").append(field).append("\":");
            appendJsonValue(sb, val);
        }
        sb.append("}");
        return sb.toString();
    }

    private void appendJsonValue(StringBuilder sb, Object val) throws IOException {
        if (val == null) {
            sb.append("null");
        } else if (val instanceof Number || val instanceof Boolean) {
            sb.append(val);
        } else if (val instanceof Object[]) {
            sb.append("[");
            Object[] arr = (Object[]) val;
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) sb.append(",");
                appendJsonValue(sb, arr[i]);
            }
            sb.append("]");
        } else {
            // escape string
            String s = val.toString()
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"");
            sb.append("\"").append(s).append("\"");
        }
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
        for (BufferedWriter w : writers.values()) {
            w.close();
        }
    }
}
