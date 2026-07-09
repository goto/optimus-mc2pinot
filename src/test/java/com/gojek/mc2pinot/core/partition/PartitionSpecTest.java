package com.gojek.mc2pinot.core.partition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PartitionSpecTest {

    private final PartitionFunction fn = new MurmurPartitionFunction();

    private static String key(int i) {
        return "user-" + i + "-key";
    }

    @Test
    void countReturnsSegmentCount() {
        assertEquals(7, new PartitionSpec("col", 4, 7).count());
    }

    @ParameterizedTest
    @CsvSource({"4,4", "8,3", "3,8", "1,5", "5,1", "16,16", "10,7", "7,10"})
    void segmentIdAlwaysInRange(int p, int s) {
        PartitionSpec spec = new PartitionSpec("col", p, s);
        for (int i = 0; i < 5000; i++) {
            int seg = spec.segmentOf(key(i), fn);
            assertTrue(seg >= 0 && seg < s, "seg=" + seg + " out of [0," + s + ") for P=" + p + " S=" + s);
        }
    }

    @ParameterizedTest
    @CsvSource({"4,4", "8,3", "3,8", "1,5", "5,1", "10,7", "7,10"})
    void deterministicByValue(int p, int s) {
        PartitionSpec spec = new PartitionSpec("col", p, s);
        for (int i = 0; i < 1000; i++) {
            assertEquals(spec.segmentOf(key(i), fn), spec.segmentOf(key(i), fn));
        }
    }

    @Test
    void sEqualsPIsIdentityToPid() {
        PartitionSpec spec = new PartitionSpec("col", 8, 8);
        for (int i = 0; i < 2000; i++) {
            assertEquals(fn.partition(key(i), 8), spec.segmentOf(key(i), fn));
        }
    }

    @Test
    void sLessThanPFoldsByModulo() {
        int p = 12, s = 5;
        PartitionSpec spec = new PartitionSpec("col", p, s);
        for (int i = 0; i < 2000; i++) {
            int pid = fn.partition(key(i), p);
            assertEquals(pid % s, spec.segmentOf(key(i), fn));
        }
    }

    /**
     * Pruning invariant: for S &gt; P every physical segment must carry records from exactly
     * one Pinot partition id, otherwise a value-filter query cannot be pruned to a pid.
     */
    @Test
    void sGreaterThanPKeepsOnePidPerSegment() {
        int p = 3, s = 8;
        PartitionSpec spec = new PartitionSpec("col", p, s);
        Map<Integer, Integer> segToPid = new HashMap<>();
        for (int i = 0; i < 20000; i++) {
            int pid = fn.partition(key(i), p);
            int seg = spec.segmentOf(key(i), fn);
            Integer prev = segToPid.putIfAbsent(seg, pid);
            if (prev != null) {
                assertEquals(prev.intValue(), pid,
                        "segment " + seg + " mixes pids " + prev + " and " + pid);
            }
        }
    }

    /** Every segment id in [0,S) should be reachable (no gaps) given enough distinct keys. */
    @ParameterizedTest
    @CsvSource({"3,8", "2,5", "4,4", "12,5"})
    void allSegmentsAreUsed(int p, int s) {
        PartitionSpec spec = new PartitionSpec("col", p, s);
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 50000; i++) {
            seen.add(spec.segmentOf(key(i), fn));
        }
        for (int seg = 0; seg < s; seg++) {
            assertTrue(seen.contains(seg), "segment " + seg + " never used for P=" + p + " S=" + s);
        }
    }

    /** Records should be reasonably uniform across segments when keys divide evenly (S|P or P|S). */
    @ParameterizedTest
    @CsvSource({"8,8", "12,4", "3,9", "2,8"})
    void distributionIsRoughlyUniformWhenDivisible(int p, int s) {
        PartitionSpec spec = new PartitionSpec("col", p, s);
        int n = 200000;
        int[] counts = new int[s];
        for (int i = 0; i < n; i++) {
            counts[spec.segmentOf(key(i), fn)]++;
        }
        double expected = (double) n / s;
        for (int c : counts) {
            double ratio = c / expected;
            assertTrue(ratio > 0.85 && ratio < 1.15,
                    "segment count " + c + " deviates too far from expected " + expected
                            + " (P=" + p + " S=" + s + ")");
        }
    }
}
