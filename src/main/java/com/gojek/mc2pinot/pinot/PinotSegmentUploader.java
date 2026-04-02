package com.gojek.mc2pinot.pinot;

import com.gojek.mc2pinot.core.SegmentInfo;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class PinotSegmentUploader {

    private static final Logger LOG = Logger.getLogger(PinotSegmentUploader.class.getName());

    private final PinotClient pinotClient;

    public PinotSegmentUploader(PinotClient pinotClient) {
        this.pinotClient = pinotClient;
    }

    public void upload(List<SegmentInfo> segments, String tableName) throws IOException {
        for (SegmentInfo segment : segments) {
            LOG.info("sink(pinot): trigger upload for segment " + segment.segmentName());
            if (segment.localPath() != null) {
                pinotClient.triggerUpload(segment.localPath(), tableName);
            } else if (segment.remoteURI() != null) {
                pinotClient.triggerUploadFromUri(segment.remoteURI(), tableName);
            } else {
                throw new IllegalArgumentException(
                        "SegmentInfo has neither a local path nor a remote URI: " + segment.segmentName());
            }
        }
    }
}

