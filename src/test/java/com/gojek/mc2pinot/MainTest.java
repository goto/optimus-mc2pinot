package com.gojek.mc2pinot;

import org.junit.jupiter.api.Test;

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
}
