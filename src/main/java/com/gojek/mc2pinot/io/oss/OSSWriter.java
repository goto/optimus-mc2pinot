package com.gojek.mc2pinot.io.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.UploadFileRequest;
import com.gojek.mc2pinot.io.Writer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

public class OSSWriter implements Writer {
    private static final long PART_SIZE = 100 * 1024 * 1024L; // 100 MB

    private final OSS ossClient;
    private final String ossURI;
    private final int writerTaskNumber;

    public OSSWriter(OSS ossClient, String ossURI, int writerTaskNumber) {
        this.ossClient = ossClient;
        this.ossURI = ossURI;
        this.writerTaskNumber = writerTaskNumber;
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
        uploadRequest.setTaskNum(this.writerTaskNumber);
        uploadRequest.setEnableCheckpoint(false);

        try {
            ossClient.uploadFile(uploadRequest);
        } catch (Throwable t) {
            throw new IOException("Multipart upload failed for key: " + fullKey, t);
        }

        return "oss://" + bucket + "/" + fullKey;
    }
}

