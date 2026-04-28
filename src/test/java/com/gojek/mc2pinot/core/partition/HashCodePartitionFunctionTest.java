package com.gojek.mc2pinot.core.partition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class HashCodePartitionFunctionTest {

    private final PartitionFunction function = new HashCodePartitionFunction();

    @Test
    void shouldReturnZeroForSinglePartition() {
        assertEquals(0, function.partition("anyValue", 1));
    }

    @Test
    void shouldReturnNonNegativeResult() {
        int result = function.partition("someValue", 4);
        assertTrue(result >= 0 && result < 4);
    }

    @Test
    void shouldBeConsistentForSameInput() {
        int first = function.partition("hello", 8);
        int second = function.partition("hello", 8);
        assertEquals(first, second);
    }

    @Test
    void shouldHandleEmptyString() {
        int result = function.partition("", 4);
        assertTrue(result >= 0 && result < 4);
    }

    @Test
    void shouldNeverReturnNegativeForHighBitHash() {
        String value = "f5a5a608";
        int result = function.partition(value, 10);
        assertTrue(result >= 0 && result < 10);
    }

    @Test
    void shouldReturnZeroPartitionForIntegerMinValueHashCode() {
        String value = "polygenelubricants";
        assertEquals(Integer.MIN_VALUE, value.hashCode());
        assertEquals(0, function.partition(value, 9));
    }

    @ParameterizedTest
    @CsvSource({
        "alpha,   5, 3",
        "bravo,   5, 3",
        "charlie, 7, 6",
        "delta,   7, 1",
        "echo,    3, 1",
        "foxtrot, 10, 2"
    })
    void shouldMatchPinotHashCodePartitionFormula(String value, int numPartitions, int expectedPartition) {
        int h = value.hashCode();
        int expected = (h == Integer.MIN_VALUE ? 0 : Math.abs(h)) % numPartitions;
        assertEquals(expected, expectedPartition, "test data is wrong for: " + value);
        assertEquals(expectedPartition, function.partition(value, numPartitions));
    }
}

