package com.gojek.mc2pinot.io.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;
import com.gojek.mc2pinot.io.Cleaner;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

public class OSSCleaner implements Cleaner {

    private static final Logger LOG = Logger.getLogger(OSSCleaner.class.getName());

    private final OSS ossClient;

    public OSSCleaner(OSS ossClient) {
        this.ossClient = ossClient;
    }

    @Override
    public void clean(String destinationURI) throws IOException {
        LOG.info("transient(oss): clean destination " + destinationURI);
        URI uri = URI.create(destinationURI);
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

            if (listing.getObjectSummaries().isEmpty() && !listing.isTruncated()) {
                LOG.info("transient(oss): destination is empty, nothing to clean");
                return;
            }

            listing.getObjectSummaries().forEach(s -> ossClient.deleteObject(bucket, s.getKey()));

            hasMore = listing.isTruncated();
            marker = listing.getNextMarker();
        }
    }
}

