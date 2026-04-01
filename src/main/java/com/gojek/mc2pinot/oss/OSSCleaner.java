package com.gojek.mc2pinot.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;

import java.net.URI;
import java.util.logging.Logger;

public class OSSCleaner {

    private static final Logger LOG = Logger.getLogger(OSSCleaner.class.getName());

    private final OSS ossClient;

    public OSSCleaner(OSS ossClient) {
        this.ossClient = ossClient;
    }

    public void clean(String ossURI) {
        LOG.info("transient(oss): clean destination " + ossURI);
        URI uri = URI.create(ossURI);
        String bucket = uri.getHost();
        String prefix = uri.getPath().substring(1);
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

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
            listing.getObjectSummaries().forEach(s -> ossClient.deleteObject(bucket, s.getKey()));

            hasMore = listing.isTruncated();
            marker = listing.getNextMarker();
        }
    }
}

