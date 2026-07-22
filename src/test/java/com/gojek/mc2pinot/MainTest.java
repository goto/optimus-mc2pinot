package com.gojek.mc2pinot;

import org.apache.pinot.spi.config.table.TableConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MainTest {

    private static final String HEX_SUFFIX_PATTERN = "-[0-9a-f]{8}$";

    @Test
    void buildSegmentFolderURI_withDeepStorageURI() {
        String result = Main.buildSegmentFolderURI(
                "oss://bucket/pinot-data", "myTable_OFFLINE", "20260427");
        assertTrue(result.matches(
                "oss://bucket/pinot-data/myTable_OFFLINE/segments_20260427" + HEX_SUFFIX_PATTERN));
    }

    @Test
    void buildSegmentFolderURI_trailingSlashStripped() {
        String result = Main.buildSegmentFolderURI(
                "oss://bucket/pinot-data/", "myTable_OFFLINE", "20260427");
        assertTrue(result.matches(
                "oss://bucket/pinot-data/myTable_OFFLINE/segments_20260427" + HEX_SUFFIX_PATTERN));
    }

    @Test
    void buildSegmentFolderURI_nullDeepStorage_usesLocalDefault() {
        String result = Main.buildSegmentFolderURI(null, "myTable_OFFLINE", "20260427");
        assertTrue(result.matches(
                "file:///tmp/mc2pinot/myTable_OFFLINE/segments_20260427" + HEX_SUFFIX_PATTERN));
    }

    @Test
    void buildSegmentFolderURI_blankDeepStorage_usesLocalDefault() {
        String result = Main.buildSegmentFolderURI("  ", "myTable_OFFLINE", "20260427");
        assertTrue(result.matches(
                "file:///tmp/mc2pinot/myTable_OFFLINE/segments_20260427" + HEX_SUFFIX_PATTERN));
    }

    @Test
    void buildSegmentFolderURI_producesUniqueSuffixes() {
        String a = Main.buildSegmentFolderURI("oss://b/d", "t", "k");
        String b = Main.buildSegmentFolderURI("oss://b/d", "t", "k");
        assertNotEquals(a, b);
    }

    @Test
    void parseTableConfig_extractsOfflineFromTableConfigsWrapper() throws Exception {
        String tableConfigJson = readResource("/test-tableConfig.json");
        String json = "{\"tableName\":\"test_table\","
                + "\"offline\":" + tableConfigJson + ","
                + "\"realtime\":null,"
                + "\"schema\":{\"schemaName\":\"test_table\"}}";

        TableConfig config = Main.parseTableConfig(json);

        assertEquals("test_table_OFFLINE", config.getTableName());
    }

    @Test
    void parseTableConfig_fallsBackToRealtime() throws Exception {
        String tableConfigJson = readResource("/test-tableConfig.json");
        String json = "{\"tableName\":\"test_table\","
                + "\"offline\":null,"
                + "\"realtime\":" + tableConfigJson + "}";

        TableConfig config = Main.parseTableConfig(json);

        assertEquals("test_table_OFFLINE", config.getTableName());
    }

    @Test
    void parseTableConfig_handlesPlainTableConfig() throws Exception {
        String json = readResource("/test-tableConfig.json");

        TableConfig config = Main.parseTableConfig(json);

        assertEquals("test_table_OFFLINE", config.getTableName());
    }

    private static String readResource(String name) throws Exception {
        try (var is = MainTest.class.getResourceAsStream(name)) {
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
