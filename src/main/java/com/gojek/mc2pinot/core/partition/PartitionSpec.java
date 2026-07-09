package com.gojek.mc2pinot.core.partition;

/**
 * Describes how records are mapped onto physical segments.
 *
 * <p>{@code partitionCount} (P) is the table's logical partition count taken from the
 * {@code SegmentPartitionConfig}; it drives the Pinot partition id used for query-time
 * partition pruning and must not change with the segment count.
 *
 * <p>{@code segmentCount} (S) is the number of physical segment files to emit, configured
 * via {@code PINOT__SEGMENT_COUNT}. It is decoupled from P; the actual record-to-segment
 * assignment lives in {@link SegmentAssigner}.
 */
public record PartitionSpec(String column, int partitionCount, int segmentCount) {

    /** Number of physical output segments. */
    public int count() {
        return segmentCount;
    }
}
