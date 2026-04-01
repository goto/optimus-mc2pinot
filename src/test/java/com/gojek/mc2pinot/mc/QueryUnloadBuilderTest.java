package com.gojek.mc2pinot.mc;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryUnloadBuilderTest {

    private static final String SAMPLE_QUERY = "SELECT * FROM my_table WHERE dt = '2025-01-01'";
    private static final String SAMPLE_ROLE_ARN = "acs:ram::123456789:role/aliyunodpsdefaultrole";
    private static final String SAMPLE_OSS_URI = "oss://oss-ap-southeast-5-internal.aliyuncs.com/my-bucket/data";

    @Test
    void shouldBuildJsonUnloadStatement() {
        String result = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("json")
                .toString();

        assertTrue(result.contains("UNLOAD FROM"));
        assertTrue(result.contains(SAMPLE_QUERY));
        assertTrue(result.contains("LOCATION '" + SAMPLE_OSS_URI + "'"));
        assertTrue(result.contains("org.apache.hive.hcatalog.data.JsonSerDe"));
        assertTrue(result.contains(SAMPLE_ROLE_ARN));
        assertTrue(result.contains("STORED AS TEXTFILE"));
        assertTrue(result.contains("odps.external.data.enable.extension"));
    }

    @Test
    void shouldReturnJsonHints() {
        Map<String, String> hints = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("json")
                .getHints();

        assertEquals("true", hints.get("odps.sql.hive.compatible"));
        assertEquals("true", hints.get("odps.sql.split.hive.bridge"));
    }

    @Test
    void shouldBuildParquetUnloadStatement() {
        String result = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("parquet")
                .toString();

        assertTrue(result.contains("UNLOAD FROM"));
        assertTrue(result.contains(SAMPLE_QUERY));
        assertTrue(result.contains("LOCATION '" + SAMPLE_OSS_URI + "'"));
        assertTrue(result.contains("ParquetHiveSerDe"));
        assertTrue(result.contains(SAMPLE_ROLE_ARN));
        assertTrue(result.contains("STORED AS PARQUET"));
        assertTrue(result.contains("mcfed.parquet.compression"));
        assertTrue(result.contains("SNAPPY"));
    }

    @Test
    void shouldReturnParquetHints() {
        Map<String, String> hints = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("parquet")
                .getHints();

        assertEquals("256", hints.get("odps.stage.mapper.split.size"));
    }

    @Test
    void shouldThrowForAvroFormat() {
        QueryUnloadBuilder builder = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("avro");

        assertThrows(UnsupportedOperationException.class, builder::toString);
    }

    @Test
    void shouldThrowForUnknownFormat() {
        QueryUnloadBuilder builder = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("csv");

        assertThrows(IllegalArgumentException.class, builder::toString);
    }

    @Test
    void shouldThrowWhenQueryNotSet() {
        QueryUnloadBuilder builder = new QueryUnloadBuilder()
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("json");

        IllegalStateException ex = assertThrows(IllegalStateException.class, builder::toString);
        assertTrue(ex.getMessage().contains("Query"));
    }

    @Test
    void shouldThrowWhenRoleArnNotSet() {
        QueryUnloadBuilder builder = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("json");

        IllegalStateException ex = assertThrows(IllegalStateException.class, builder::toString);
        assertTrue(ex.getMessage().contains("Role ARN"));
    }

    @Test
    void shouldThrowWhenOSSDestinationNotSet() {
        QueryUnloadBuilder builder = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOutputFormat("json");

        IllegalStateException ex = assertThrows(IllegalStateException.class, builder::toString);
        assertTrue(ex.getMessage().contains("OSS destination"));
    }

    @Test
    void shouldThrowWhenOutputFormatNotSet() {
        QueryUnloadBuilder builder = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI);

        IllegalStateException ex = assertThrows(IllegalStateException.class, builder::toString);
        assertTrue(ex.getMessage().contains("Output format"));
    }

    @Test
    void shouldBeCaseInsensitiveOnFormat() {
        String result = new QueryUnloadBuilder()
                .setQuery(SAMPLE_QUERY)
                .setRoleARN(SAMPLE_ROLE_ARN)
                .setOSSDestinationURI(SAMPLE_OSS_URI)
                .setOutputFormat("JSON")
                .toString();

        assertTrue(result.contains("STORED AS TEXTFILE"));
    }
}

