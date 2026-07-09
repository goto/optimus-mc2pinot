package com.gojek.mc2pinot.core.partition;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Maps records to a physical segment id in {@code [0, S)} while keeping Pinot partition pruning
 * intact. The partition id is always computed with P (the table's partition count), and every
 * emitted segment carries exactly one partition id, so a value-filter query still prunes to a pid.
 *
 * <ul>
 *   <li><b>S &le; P</b> — several partition ids fold into one segment ({@code pid % S}); pure
 *       function of the value, so the same key always lands in the same segment.</li>
 *   <li><b>S &gt; P</b> — each partition id owns a contiguous block of {@code ~S/P} segments, and
 *       its records are spread across that block by a <b>per-partition round-robin counter</b>
 *       (Option B). This guarantees every segment receives records as long as each partition has at
 *       least as many records as it has segments, and yields near-uniform segment sizes. The cost
 *       is that the same key value may land in different segments of its partition's block.</li>
 * </ul>
 *
 * <p>The round-robin counters are shared and thread-safe: a single instance must be shared by all
 * concurrent file splitters in a run, otherwise the fill guarantee weakens (many small inputs could
 * each place their few records in the same sub-segment). Assignment for {@code S > P} is therefore
 * order/scheduling dependent rather than value-deterministic.
 */
public class SegmentAssigner {

    private final PartitionFunction fn;
    private final int p;
    private final int s;
    private final int base;
    private final int extra;
    /** Per-partition round-robin counters; only allocated (and used) when {@code S > P}. */
    private final AtomicIntegerArray counters;

    public SegmentAssigner(PartitionSpec spec, PartitionFunction fn) {
        this.fn = fn;
        this.p = Math.max(spec.partitionCount(), 1);
        this.s = Math.max(spec.segmentCount(), 1);
        this.base = s / p;
        this.extra = s % p;
        this.counters = s > p ? new AtomicIntegerArray(p) : null;
    }

    /** Returns the segment id in {@code [0, S)} for {@code value}. Advances state when {@code S > P}. */
    public int assign(String value) {
        int pid = fn.partition(value, p);

        if (s <= p) {
            return pid % s;
        }

        // S > P: round-robin this partition's records across its contiguous block of segments.
        int cnt = base + (pid < extra ? 1 : 0);
        // floorMod keeps the result in [0, cnt) even after the counter overflows to negative.
        int sub = Math.floorMod(counters.getAndIncrement(pid), cnt);
        return pid * base + Math.min(pid, extra) + sub;
    }
}
