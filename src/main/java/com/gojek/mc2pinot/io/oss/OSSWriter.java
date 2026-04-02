package com.gojek.mc2pinot.io.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.UploadFileRequest;
import com.gojek.mc2pinot.io.Writer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

public class OSSWriter implements Writer {
    private static final long PART_SIZE = 100 * 1024 * 1024L; // 100 MB
    private static final int TASK_NUM = 5;

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

        String fullKey = basePath + objectKey;

        UploadFileRequest uploadRequest = new UploadFileRequest(bucket, fullKey);
        uploadRequest.setUploadFile(localFile.toAbsolutePath().toString());
        uploadRequest.setPartSize(PART_SIZE);
        uploadRequest.setTaskNum(TASK_NUM);
        uploadRequest.setEnableCheckpoint(false);

        try {
            ossClient.uploadFile(uploadRequest);
        } catch (Throwable t) {
            throw new IOException("Multipart upload failed for key: " + fullKey, t);
        }

        return "oss://" + bucket + "/" + fullKey;
    }
}

