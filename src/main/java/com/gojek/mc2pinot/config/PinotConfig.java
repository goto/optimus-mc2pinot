package com.gojek.mc2pinot.config;

import java.net.URI;
import java.util.Map;

public class PinotConfig {

    private final String host;
    private final String segmentKey;
    private final String inputFormat;
    private final String schemaFilePath;
    private final String tableConfigFilePath;
    private final String deepStorageURI;
    private final OSSConfig deepStorageOssConfig;

    public PinotConfig(Map<String, String> env) {
        this.host = ConfigHelper.requireNonEmpty(env, Constant.PINOT_HOST);
        this.segmentKey = ConfigHelper.requireNonEmpty(env, Constant.PINOT_SEGMENT_KEY);
        this.inputFormat = ConfigHelper.requireNonEmpty(env, Constant.PINOT_INPUT_FORMAT);
        this.schemaFilePath = ConfigHelper.requireNonEmpty(env, Constant.PINOT_SCHEMA_FILE_PATH);
        this.tableConfigFilePath = ConfigHelper.requireNonEmpty(env, Constant.PINOT_TABLE_CONFIG_FILE_PATH);
        this.deepStorageURI = env.get(Constant.PINOT_DEEP_STORAGE_URI);
        String scheme = resolveScheme(deepStorageURI);
        this.deepStorageOssConfig = "oss".equals(scheme) ? new OSSConfig(env) : null;
    }

    public String getHost() {
        return host;
    }

    public String getSegmentKey() {
        return segmentKey;
    }

    public String getInputFormat() {
        return inputFormat.toLowerCase();
    }

    public String getSchemaFilePath() {
        return schemaFilePath;
    }

    public String getTableConfigFilePath() {
        return tableConfigFilePath;
    }

    public String getDeepStorageURI() {
        return deepStorageURI;
    }

    public OSSConfig getDeepStorageOssConfig() {
        return deepStorageOssConfig;
    }

    private static String resolveScheme(String uri) {
        if (uri == null || uri.isBlank()) {
            return null;
        }
        try {
            return URI.create(uri).getScheme();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
