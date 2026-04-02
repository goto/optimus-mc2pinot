package com.gojek.mc2pinot.config;

import java.net.URI;
import java.util.Map;

public class FsConfig {

    private final String destinationURI;
    private final OSSConfig ossConfig;

    public FsConfig(Map<String, String> env) {
        this.destinationURI = ConfigHelper.requireNonEmpty(env, Constant.FS_DESTINATION_URI);

        String scheme = resolveScheme(destinationURI);
        this.ossConfig = "oss".equals(scheme) ? new OSSConfig(env) : null;
    }

    public String getDestinationURI() {
        return destinationURI;
    }

    public OSSConfig getOssConfig() {
        return ossConfig;
    }

    private static String resolveScheme(String uri) {
        try {
            return URI.create(uri).getScheme();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}

