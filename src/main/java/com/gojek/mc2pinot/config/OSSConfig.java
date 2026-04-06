package com.gojek.mc2pinot.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;

public class OSSConfig {

    private final String accessKeyId;
    private final String accessKeySecret;
    private final String endpoint;
    private final String region;
    private final String writerTaskNumber;

    public OSSConfig(Map<String, String> env) {
        String serviceAccount = ConfigHelper.requireNonEmpty(env, Constant.PINOT_DEEP_STORAGE_OSS_SERVICE_ACCOUNT);

        JsonObject json = JsonParser.parseString(serviceAccount).getAsJsonObject();
        this.accessKeyId = ConfigHelper.requireJsonField(json, Constant.PINOT_DEEP_STORAGE_OSS_SERVICE_ACCOUNT, "access_key_id");
        this.accessKeySecret = ConfigHelper.requireJsonField(json, Constant.PINOT_DEEP_STORAGE_OSS_SERVICE_ACCOUNT, "access_key_secret");
        this.endpoint = ConfigHelper.requireJsonField(json, Constant.PINOT_DEEP_STORAGE_OSS_SERVICE_ACCOUNT, "endpoint");
        this.region = ConfigHelper.requireJsonField(json, Constant.PINOT_DEEP_STORAGE_OSS_SERVICE_ACCOUNT, "region");

        this.writerTaskNumber = ConfigHelper.optionalWithDefault(env, Constant.PINOT_DEEP_STORAGE_OSS_WRITER_TASK_NUMBER, "5");
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public Integer getWriterTaskNumber() {
        try {
            return Integer.parseInt(writerTaskNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number format for " + Constant.PINOT_DEEP_STORAGE_OSS_WRITER_TASK_NUMBER + ": " + writerTaskNumber);
        }
    }
}
