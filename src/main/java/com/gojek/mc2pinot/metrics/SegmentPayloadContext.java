package com.gojek.mc2pinot.metrics;

public record SegmentPayloadContext(
        long inputRecordCount,
        long inputRecordSize,
        String segmentName,
        long outputRecordCount,
        long outputRecordSize
) {
}

