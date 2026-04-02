package com.gojek.mc2pinot.io.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.gojek.mc2pinot.io.Reader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public class OSSReader implements Reader {

    private final OSS ossClient;
    private final String ossURI;
    private final List<Path> tempFiles = new ArrayList<>();

    public OSSReader(OSS ossClient, String ossURI) {
        this.ossClient = ossClient;
        this.ossURI = ossURI;
    }

    @Override
    public List<Path> read() throws IOException {
        URI uri = URI.create(ossURI);
        String bucket = uri.getHost();
        String prefix = uri.getPath().substring(1);
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        Path tempDir = Files.createTempDirectory("oss-download-");
        tempFiles.add(tempDir);

        List<Path> downloadedFiles = new ArrayList<>();
        String marker = null;
        boolean hasMore = true;

        while (hasMore) {
            ListObjectsRequest request = new ListObjectsRequest(bucket);
            request.setPrefix(prefix);
            request.setMaxKeys(1000);
            if (marker != null) {
                request.setMarker(marker);
            }

            ObjectListing listing = ossClient.listObjects(request);

            for (OSSObjectSummary summary : listing.getObjectSummaries()) {
                String key = summary.getKey();
                if (key.endsWith("/") || key.startsWith(prefix + "segments/")) {
                    continue;
                }

                String fileName = key.substring(key.lastIndexOf('/') + 1);
                Path localFile = tempDir.resolve(fileName);

                try (InputStream stream = ossClient.getObject(bucket, key).getObjectContent()) {
                    Files.copy(stream, localFile, StandardCopyOption.REPLACE_EXISTING);
                }

                downloadedFiles.add(localFile);
            }

            hasMore = listing.isTruncated();
            marker = listing.getNextMarker();
        }

        return downloadedFiles;
    }

    @Override
    public void close() throws IOException {
        for (Path path : tempFiles) {
            deleteRecursively(path);
        }
        tempFiles.clear();
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var entries = Files.list(path)) {
                for (Path entry : entries.toList()) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}

