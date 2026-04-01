package com.gojek.mc2pinot.mc;

import java.util.HashMap;
import java.util.Map;

public class QueryUnloadBuilder {

    private String query;
    private String roleARN;
    private String ossDestinationURI;
    private String outputFormat;

    public QueryUnloadBuilder setQuery(String query) {
        this.query = query;
        return this;
    }

    public QueryUnloadBuilder setRoleARN(String roleARN) {
        this.roleARN = roleARN;
        return this;
    }

    public QueryUnloadBuilder setOSSDestinationURI(String ossDestinationURI) {
        this.ossDestinationURI = ossDestinationURI;
        return this;
    }

    public QueryUnloadBuilder setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
        return this;
    }

    @Override
    public String toString() {
        validate();
        return switch (outputFormat.toLowerCase()) {
            case "json" -> buildJsonUnload();
            case "parquet" -> buildParquetUnload();
            case "avro" -> throw new UnsupportedOperationException("AVRO unload format is not yet supported");
            default -> throw new IllegalArgumentException("Unknown output format: " + outputFormat);
        };
    }

    public Map<String, String> getHints() {
        validate();
        return switch (outputFormat.toLowerCase()) {
            case "json" -> new HashMap<>(Map.of(
                    "odps.sql.hive.compatible", "true",
                    "odps.sql.split.hive.bridge", "true"
            ));
            case "parquet" -> new HashMap<>(Map.of(
                    "odps.stage.mapper.split.size", "256"
            ));
            default -> new HashMap<>();
        };
    }

    private String buildJsonUnload() {
        return """
                UNLOAD FROM
                (
                %s
                )
                INTO
                LOCATION '%s'
                ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
                WITH SERDEPROPERTIES ('odps.properties.rolearn'='%s')
                STORED AS TEXTFILE
                PROPERTIES ('odps.external.data.enable.extension'='true');""".formatted(query, ossDestinationURI, roleARN);
    }

    private String buildParquetUnload() {
        return """
                UNLOAD FROM
                (
                %s
                )
                INTO
                LOCATION '%s'
                ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                WITH SERDEPROPERTIES ('odps.properties.rolearn'='%s')
                STORED AS PARQUET
                PROPERTIES('mcfed.parquet.compression'='SNAPPY');""".formatted(query, ossDestinationURI, roleARN);
    }

    private void validate() {
        if (query == null || query.isBlank()) {
            throw new IllegalStateException("Query must be set");
        }
        if (roleARN == null || roleARN.isBlank()) {
            throw new IllegalStateException("Role ARN must be set");
        }
        if (ossDestinationURI == null || ossDestinationURI.isBlank()) {
            throw new IllegalStateException("OSS destination URI must be set");
        }
        if (outputFormat == null || outputFormat.isBlank()) {
            throw new IllegalStateException("Output format must be set");
        }
    }
}

