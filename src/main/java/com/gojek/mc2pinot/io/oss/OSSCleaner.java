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
    public void clean(String uri) throws IOException {
        URI parsed = URI.create(uri);
        String bucket = parsed.getHost();
        String path = parsed.getPath().substring(1);

        if (uri.endsWith("/")) {
            cleanPrefix(bucket, path);
        } else {
            LOG.info("transient(oss): delete segment " + uri);
            ossClient.deleteObject(bucket, path);
        }
    }

    private void cleanPrefix(String bucket, String prefix) {
        LOG.info("transient(oss): clean prefix oss://" + bucket + "/" + prefix);
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
