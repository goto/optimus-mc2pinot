package com.gojek.mc2pinot.pinot;

import java.io.IOException;
import java.nio.file.Path;

public interface PinotClient {

    String triggerUpload(Path segmentFile, String tableName) throws IOException;

    String triggerUploadFromUri(String uri, String tableName, String customPayload) throws IOException;

    String getSchema(String tableName) throws IOException;

    String getTableConfig(String tableName) throws IOException;
}

