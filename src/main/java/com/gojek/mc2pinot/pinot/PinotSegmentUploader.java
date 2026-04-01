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
            pinotClient.triggerUploadFromUri(segment.ossURI(), tableName);
        }
    }
}

