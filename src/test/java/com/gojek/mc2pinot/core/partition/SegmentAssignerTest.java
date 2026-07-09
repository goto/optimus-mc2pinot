package com.gojek.mc2pinot.core.partition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SegmentAssignerTest {

    private static String key(int i) {
        return "user-" + i + "-key";
    }

    private static SegmentAssigner assigner(PartitionFunction fn, int p, int s) {
        return new SegmentAssigner(new PartitionSpec("col", p, s), fn);
    }

    // Mirrors the block math in SegmentAssigner for verifying the pruning invariant.
    private static int blockBase(int p, int s) { return s / p; }
    private static int blockExtra(int p, int s) { return s % p; }
    private static int blockCount(int p, int s, int pid) {
        return blockBase(p, s) + (pid < blockExtra(p, s) ? 1 : 0);
    }
    private static int blockOffset(int p, int s, int pid) {
        return pid * blockBase(p, s) + Math.min(pid, blockExtra(p, s));
    }

    @ParameterizedTest
    @CsvSource({"4,4", "8,3", "3,8", "1,5", "5,1", "16,16", "10,7", "7,10", "8,32"})
    void segmentIdAlwaysInRange(int p, int s) {
        SegmentAssigner a = assigner(new MurmurPartitionFunction(), p, s);
        for (int i = 0; i < 5000; i++) {
            int seg = a.assign(key(i));
            assertTrue(seg >= 0 && seg < s, "seg=" + seg + " out of [0," + s + ") for P=" + p + " S=" + s);
        }
    }

    @Test
    void sEqualsPIsIdentityToPid() {
        PartitionFunction fn = new MurmurPartitionFunction();
        SegmentAssigner a = assigner(fn, 8, 8);
        for (int i = 0; i < 2000; i++) {
            assertEquals(fn.partition(key(i), 8), a.assign(key(i)));
        }
    }

    @Test
    void sLessThanPFoldsByModuloAndIsValueDeterministic() {
        int p = 12, s = 5;
        PartitionFunction fn = new MurmurPartitionFunction();
        SegmentAssigner a = assigner(fn, p, s);
        for (int i = 0; i < 2000; i++) {
            int pid = fn.partition(key(i), p);
            assertEquals(pid % s, a.assign(key(i)));
            // fold case is stateless, so repeated calls give the same segment
            assertEquals(pid % s, a.assign(key(i)));
        }
    }

    /**
     * Pruning invariant (Option B, S&gt;P): every segment a record can land in must belong to that
     * record's single partition id — i.e. it falls inside that pid's contiguous block. Since the
     * blocks are disjoint, this proves each segment holds exactly one partition id.
     */
    @ParameterizedTest
    @CsvSource({"3,8", "8,32", "2,5", "8,9"})
    void sGreaterThanPKeepsOnePidPerSegment(int p, int s) {
        PartitionFunction fn = new MurmurPartitionFunction();
        SegmentAssigner a = assigner(fn, p, s);
        for (int i = 0; i < 20000; i++) {
            int pid = fn.partition(key(i), p);
            int seg = a.assign(key(i));
            int lo = blockOffset(p, s, pid);
            int hi = lo + blockCount(p, s, pid);
            assertTrue(seg >= lo && seg < hi,
                    "seg " + seg + " outside pid " + pid + " block [" + lo + "," + hi + ")");
        }
    }

    /** Round-robin fills every segment once each pid has at least as many records as it has segments. */
    @ParameterizedTest
    @CsvSource({"3,8", "8,32", "2,5", "12,5", "8,9"})
    void allSegmentsFilledGivenEnoughRecords(int p, int s) {
        SegmentAssigner a = assigner(new MurmurPartitionFunction(), p, s);
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 100000; i++) {
            seen.add(a.assign(key(i)));
        }
        for (int seg = 0; seg < s; seg++) {
            assertTrue(seen.contains(seg), "segment " + seg + " never used for P=" + p + " S=" + s);
        }
    }

    /**
     * Regression for the original bug: with the HashCode partition function (poor low-bit entropy)
     * and S&gt;P, round-robin must still fill every segment — the old salted-hash sub-bucket left
     * S-P segments empty.
     */
    @Test
    void sGreaterThanPFillsAllSegmentsWithHashCodeFunction() {
        int p = 8, s = 32;
        SegmentAssigner a = assigner(new HashCodePartitionFunction(), p, s);
        int n = 200000;
        int[] counts = new int[s];
        for (int i = 0; i < n; i++) {
            counts[a.assign(String.valueOf(100000000L + i))]++; // numeric keys: worst case for String.hashCode
        }
        double expected = (double) n / s;
        for (int seg = 0; seg < s; seg++) {
            assertTrue(counts[seg] > 0, "segment " + seg + " empty with HashCode function");
            double ratio = counts[seg] / expected;
            assertTrue(ratio > 0.85 && ratio < 1.15,
                    "segment " + seg + " count " + counts[seg] + " far from expected " + expected);
        }
    }

    /** Round-robin gives near-perfect balance: within a pid, sub-segments differ by at most one record. */
    @Test
    void roundRobinIsNearPerfectlyBalancedWithinPid() {
        int p = 4, s = 12; // 3 segments per pid
        PartitionFunction fn = new MurmurPartitionFunction();
        SegmentAssigner a = assigner(fn, p, s);
        long[] counts = new long[s];
        int n = 240000;
        for (int i = 0; i < n; i++) {
            counts[a.assign(key(i))]++;
        }
        for (int pid = 0; pid < p; pid++) {
            long min = Long.MAX_VALUE, max = 0;
            for (int sub = 0; sub < blockCount(p, s, pid); sub++) {
                long c = counts[blockOffset(p, s, pid) + sub];
                min = Math.min(min, c);
                max = Math.max(max, c);
            }
            assertTrue(max - min <= 1, "pid " + pid + " sub-segments unbalanced: min=" + min + " max=" + max);
        }
    }

    /** Same key value always resolves to the same partition (its segments share one pid). */
    @Test
    void sameKeyStaysWithinOnePartitionBlock() {
        int p = 8, s = 32;
        PartitionFunction fn = new MurmurPartitionFunction();
        SegmentAssigner a = assigner(fn, p, s);
        for (int i = 0; i < 5000; i++) {
            String v = key(i);
            int pid = fn.partition(v, p);
            int lo = blockOffset(p, s, pid);
            int hi = lo + blockCount(p, s, pid);
            for (int rep = 0; rep < 5; rep++) { // same key, several records
                int seg = a.assign(v);
                assertTrue(seg >= lo && seg < hi, "same key escaped its partition block");
            }
        }
    }

    /** The shared assigner is thread-safe: concurrent use still fills all segments and keeps pruning. */
    @Test
    void concurrentAssignmentFillsAllSegmentsAndKeepsPruning() throws Exception {
        int p = 8, s = 32;
        PartitionFunction fn = new MurmurPartitionFunction();
        SegmentAssigner a = assigner(fn, p, s);
        int threads = 4, perThread = 50000;
        Set<Integer> seen = ConcurrentHashMap.newKeySet();
        Set<String> violations = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        String v = key(base + i);
                        int pid = fn.partition(v, p);
                        int seg = a.assign(v);
                        seen.add(seg);
                        int lo = blockOffset(p, s, pid);
                        if (seg < lo || seg >= lo + blockCount(p, s, pid)) {
                            violations.add("seg " + seg + " not in pid " + pid + " block");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "threads did not finish");
        assertTrue(violations.isEmpty(), "pruning violations: " + violations);
        assertEquals(s, seen.size(), "not all segments were filled under concurrency");
    }
}
