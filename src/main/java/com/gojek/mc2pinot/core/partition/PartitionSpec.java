package com.gojek.mc2pinot.core.partition;

/**
 * Describes how records are mapped onto physical segments.
 *
 * <p>{@code partitionCount} (P) is the table's logical partition count taken from the
 * {@code SegmentPartitionConfig}; it drives the Pinot partition id used for query-time
 * partition pruning and must not change with the segment count.
 *
 * <p>{@code segmentCount} (S) is the number of physical segment files to emit, configured
 * via {@code PINOT__SEGMENT_COUNT}. It is decoupled from P: see {@link #segmentOf}.
 */
public record PartitionSpec(String column, int partitionCount, int segmentCount) {

    /** Prefix that makes the S&gt;P sub-bucket hash independent of the pid hash. */
    private static final String SUB_BUCKET_SALT = "seg#";

    /** Number of physical output segments (the id space produced by {@link #segmentOf}). */
    public int count() {
        return segmentCount;
    }

    /**
     * Maps a partition-column value to a segment id in {@code [0, segmentCount)}, keeping the
     * Pinot partition id (computed with {@code partitionCount}) consistent so partition pruning
     * still works. Deterministic by value, so the same key always lands in the same segment.
     *
     * <ul>
     *   <li><b>S &le; P</b> — several partition ids fold into one segment
     *       ({@code pid % S}); each segment carries a small, known pid set.</li>
     *   <li><b>S &gt; P</b> — each partition id is split across {@code ~S/P} segments using a
     *       salted second hash; every resulting segment still carries exactly one pid.</li>
     * </ul>
     */
    public int segmentOf(String value, PartitionFunction fn) {
        int p = Math.max(partitionCount, 1);
        int s = Math.max(segmentCount, 1);
        int pid = fn.partition(value, p);

        if (s <= p) {
            return pid % s;
        }

        // S > P: subdivide each pid across its own contiguous block of segments.
        int base = s / p;          // segments guaranteed per pid
        int extra = s % p;         // the first `extra` pids get one segment more
        int cnt = base + (pid < extra ? 1 : 0);
        // Salted so the sub-bucket hash is independent of the pid hash, yet deterministic per key.
        int sub = fn.partition(SUB_BUCKET_SALT + value, cnt);
        return pid * base + Math.min(pid, extra) + sub;
    }
}
