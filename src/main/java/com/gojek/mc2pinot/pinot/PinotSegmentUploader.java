package com.gojek.mc2pinot.pinot;

import com.gojek.mc2pinot.core.SegmentInfo;
import com.gojek.mc2pinot.io.Cleaner;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

public class PinotSegmentUploader {

    private static final Logger LOG = Logger.getLogger(PinotSegmentUploader.class.getName());

    private final PinotClient pinotClient;
    private final UploadMode uploadMode;
    private final Cleaner cleaner;
    private final long pushDelayInSeconds;

    public PinotSegmentUploader(PinotClient pinotClient, UploadMode uploadMode, Cleaner cleaner) {
        this(pinotClient, uploadMode, cleaner, 0L);
    }

    public PinotSegmentUploader(PinotClient pinotClient, UploadMode uploadMode, Cleaner cleaner,
                                long pushDelayInSeconds) {
        this.pinotClient = pinotClient;
        this.uploadMode = uploadMode;
        this.cleaner = cleaner;
        this.pushDelayInSeconds = Math.max(0L, pushDelayInSeconds);
    }

    public void upload(List<SegmentInfo> segments, String tableName,
                       Function<SegmentInfo, String> payloadSupplier) throws IOException {
        boolean first = true;
        for (SegmentInfo segment : segments) {
            if (!first && pushDelayInSeconds > 0) {
                LOG.info("sink(pinot): waiting " + pushDelayInSeconds + "s before next segment push");
                try {
                    Thread.sleep(pushDelayInSeconds * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting between segment pushes", e);
                }
            }
            first = false;
            LOG.info("sink(pinot): trigger upload for segment " + segment.segmentName());
            switch (uploadMode) {
                case URI -> {
                    if (segment.remoteURI() == null) {
                        throw new IllegalArgumentException(
                                "UploadMode is URI but segment has no remote URI: " + segment.segmentName());
                    }
                    String payload = payloadSupplier.apply(segment);
                    pinotClient.triggerUploadFromUri(segment.remoteURI(), tableName, payload);
                    cleaner.clean(segment.remoteURI());
                }
                case METADATA -> {
                    if (segment.remoteURI() == null) {
                        throw new IllegalArgumentException(
                                "UploadMode is METADATA but segment has no remote URI: " + segment.segmentName());
                    }
                    if (segment.metadataPath() == null) {
                        throw new IllegalArgumentException(
                                "UploadMode is METADATA but segment has no metadata file: " + segment.segmentName());
                    }
                    String payload = payloadSupplier.apply(segment);
                    pinotClient.triggerUploadByMetadata(
                            segment.metadataPath(), segment.remoteURI(), tableName, payload);
                    Files.deleteIfExists(segment.metadataPath());
                }
                case FILE -> {
                    if (segment.localPath() == null) {
                        throw new IllegalArgumentException(
                                "UploadMode is FILE but segment has no local path: " + segment.segmentName());
                    }
                    pinotClient.triggerUpload(segment.localPath(), tableName);
                }
            }
            if (segment.localPath() != null) {
                Files.deleteIfExists(segment.localPath());
            }
        }
    }
}

