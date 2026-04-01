package com.gojek.mc2pinot.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;

public class OSSConfig {

    private final String accessKeyId;
    private final String accessKeySecret;
    private final String endpoint;
    private final String region;
    private final String destinationURI;
    private final String roleArn;

    public OSSConfig(Map<String, String> env) {
        String serviceAccount = ConfigHelper.requireNonEmpty(env, Constant.OSS_SERVICE_ACCOUNT);
        this.destinationURI = ConfigHelper.requireNonEmpty(env, Constant.OSS_DESTINATION_URI);
        this.roleArn = ConfigHelper.requireNonEmpty(env, Constant.OSS_ROLE_ARN);

        JsonObject json = JsonParser.parseString(serviceAccount).getAsJsonObject();
        this.accessKeyId = ConfigHelper.requireJsonField(json, Constant.OSS_SERVICE_ACCOUNT, "access_key_id");
        this.accessKeySecret = ConfigHelper.requireJsonField(json, Constant.OSS_SERVICE_ACCOUNT, "access_key_secret");
        this.endpoint = ConfigHelper.requireJsonField(json, Constant.OSS_SERVICE_ACCOUNT, "endpoint");
        this.region = ConfigHelper.requireJsonField(json, Constant.OSS_SERVICE_ACCOUNT, "region");
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

    public String getDestinationURI() {
        return destinationURI;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getSegmentOutputURI() {
        String base = destinationURI.endsWith("/") ? destinationURI : destinationURI + "/";
        return base + "segments";
    }
}

