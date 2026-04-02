package com.gojek.mc2pinot.core;

import java.nio.file.Path;

public record SegmentInfo(String segmentName, String remoteURI, Path localPath) {
}

