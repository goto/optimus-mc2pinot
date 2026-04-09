package com.gojek.mc2pinot.core;

import java.util.List;

public record GenerationResult(List<SegmentInfo> segments, long inputRecordCount, long inputRecordSize) {
}


