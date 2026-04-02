package com.gojek.mc2pinot.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;

public class OSSConfig {

    private final String accessKeyId;
    private final String accessKeySecret;
    private final String endpoint;
    private final String region;

    public OSSConfig(Map<String, String> env) {
        String serviceAccount = ConfigHelper.requireNonEmpty(env, Constant.FS_OSS_SERVICE_ACCOUNT);

        JsonObject json = JsonParser.parseString(serviceAccount).getAsJsonObject();
        this.accessKeyId = ConfigHelper.requireJsonField(json, Constant.FS_OSS_SERVICE_ACCOUNT, "access_key_id");
        this.accessKeySecret = ConfigHelper.requireJsonField(json, Constant.FS_OSS_SERVICE_ACCOUNT, "access_key_secret");
        this.endpoint = ConfigHelper.requireJsonField(json, Constant.FS_OSS_SERVICE_ACCOUNT, "endpoint");
        this.region = ConfigHelper.requireJsonField(json, Constant.FS_OSS_SERVICE_ACCOUNT, "region");
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
}
