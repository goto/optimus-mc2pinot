package com.gojek.mc2pinot.oss;

import com.aliyun.oss.OSS;
import com.gojek.mc2pinot.io.Writer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

public class OSSWriter implements Writer {

    private final OSS ossClient;
    private final String ossURI;

    public OSSWriter(OSS ossClient, String ossURI) {
        this.ossClient = ossClient;
        this.ossURI = ossURI;
    }

    @Override
    public String write(String objectKey, Path localFile) throws IOException {
        URI uri = URI.create(ossURI);
        String bucket = uri.getHost();
        String basePath = uri.getPath().substring(1);
        if (!basePath.endsWith("/")) {
            basePath = basePath + "/";
        }

        String fullKey = basePath + "segments/" + objectKey;

        ossClient.putObject(bucket, fullKey, localFile.toFile());

        return "oss://" + bucket + "/" + fullKey;
    }
}

