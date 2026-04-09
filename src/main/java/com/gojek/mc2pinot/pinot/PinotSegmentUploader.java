package com.gojek.mc2pinot.pinot;

import com.gojek.mc2pinot.core.SegmentInfo;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

public class PinotSegmentUploader {

    private static final Logger LOG = Logger.getLogger(PinotSegmentUploader.class.getName());

    private final PinotClient pinotClient;
    private final UploadMode uploadMode;

    public PinotSegmentUploader(PinotClient pinotClient, UploadMode uploadMode) {
        this.pinotClient = pinotClient;
        this.uploadMode = uploadMode;
    }

    public void upload(List<SegmentInfo> segments, String tableName,
                       Function<SegmentInfo, String> payloadSupplier) throws IOException {
        for (SegmentInfo segment : segments) {
            LOG.info("sink(pinot): trigger upload for segment " + segment.segmentName());
            switch (uploadMode) {
                case URI -> {
                    if (segment.remoteURI() == null) {
                        throw new IllegalArgumentException(
                                "UploadMode is URI but segment has no remote URI: " + segment.segmentName());
                    }
                    String payload = payloadSupplier.apply(segment);
                    pinotClient.triggerUploadFromUri(segment.remoteURI(), tableName, payload);
                }
                case FILE -> {
                    if (segment.localPath() == null) {
                        throw new IllegalArgumentException(
                                "UploadMode is FILE but segment has no local path: " + segment.segmentName());
                    }
                    pinotClient.triggerUpload(segment.localPath(), tableName);
                }
            }
        }
    }
}

