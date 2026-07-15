package com.gojek.mc2pinot.pinot;

import java.io.IOException;
import java.nio.file.Path;

public interface PinotClient {

    String triggerUpload(Path segmentFile, String tableName) throws IOException;

    String triggerUploadFromUri(String uri, String tableName, String customPayload) throws IOException;

    String triggerUploadByMetadata(Path metadataFile, String uri, String tableName, String customPayload) throws IOException;

    /**
     * Triggers a reload of a single segment on the controller. When {@code forceDownload} is
     * {@code true}, servers re-download the segment from deep storage and reprocess it even if the
     * CRC is unchanged (used to make Pinot pick up a re-pushed, byte-identical segment).
     */
    String reloadSegment(String segmentName, String tableName, boolean forceDownload) throws IOException;

    String getSchema(String tableName) throws IOException;

    String getTableConfig(String tableName) throws IOException;
}

