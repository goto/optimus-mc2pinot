package com.gojek.mc2pinot.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;

public class MaxcomputeConfig {

    private final String projectName;
    private final String accessId;
    private final String accessKey;
    private final String endpoint;
    private final String queryFilePath;
    private final String ossDestinationURI;
    private final String ossRoleArn;
    private final String ossAccessKeyId;
    private final String ossAccessKeySecret;
    private final String ossEndpoint;
    private final String ossRegion;

    public MaxcomputeConfig(Map<String, String> env) {
        String serviceAccount = ConfigHelper.requireNonEmpty(env, Constant.MC_SERVICE_ACCOUNT);
        this.queryFilePath = ConfigHelper.requireNonEmpty(env, Constant.MC_QUERY_FILE_PATH);
        this.ossDestinationURI = ConfigHelper.requireNonEmpty(env, Constant.MC_OSS_DESTINATION_URI);
        this.ossRoleArn = ConfigHelper.requireNonEmpty(env, Constant.MC_OSS_ROLE_ARN);

        JsonObject json = JsonParser.parseString(serviceAccount).getAsJsonObject();
        this.projectName = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "project_name");
        this.accessId = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "access_id");
        this.accessKey = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "access_key");
        this.endpoint = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "endpoint");

        String ossServiceAccount = ConfigHelper.requireNonEmpty(env, Constant.MC_OSS_SERVICE_ACCOUNT);
        JsonObject ossJson = JsonParser.parseString(ossServiceAccount).getAsJsonObject();
        this.ossAccessKeyId = ConfigHelper.requireJsonField(ossJson, Constant.MC_OSS_SERVICE_ACCOUNT, "access_key_id");
        this.ossAccessKeySecret = ConfigHelper.requireJsonField(ossJson, Constant.MC_OSS_SERVICE_ACCOUNT, "access_key_secret");
        this.ossEndpoint = ConfigHelper.requireJsonField(ossJson, Constant.MC_OSS_SERVICE_ACCOUNT, "endpoint");
        this.ossRegion = ConfigHelper.requireJsonField(ossJson, Constant.MC_OSS_SERVICE_ACCOUNT, "region");
    }

    public String getProjectName() {
        return projectName;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getQueryFilePath() {
        return queryFilePath;
    }

    public String getOssDestinationURI() {
        return ossDestinationURI;
    }

    public String getOssRoleArn() {
        return ossRoleArn;
    }

    public String getOssAccessKeyId() {
        return ossAccessKeyId;
    }

    public String getOssAccessKeySecret() {
        return ossAccessKeySecret;
    }

    public String getOssEndpoint() {
        return ossEndpoint;
    }

    public String getOssRegion() {
        return ossRegion;
    }
}
