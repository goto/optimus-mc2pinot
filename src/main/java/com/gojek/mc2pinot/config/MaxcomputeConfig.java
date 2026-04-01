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

    public MaxcomputeConfig(Map<String, String> env) {
        String serviceAccount = ConfigHelper.requireNonEmpty(env, Constant.MC_SERVICE_ACCOUNT);
        this.queryFilePath = ConfigHelper.requireNonEmpty(env, Constant.MC_QUERY_FILE_PATH);

        JsonObject json = JsonParser.parseString(serviceAccount).getAsJsonObject();
        this.projectName = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "project_name");
        this.accessId = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "access_id");
        this.accessKey = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "access_key");
        this.endpoint = ConfigHelper.requireJsonField(json, Constant.MC_SERVICE_ACCOUNT, "endpoint");
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
}

