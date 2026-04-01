package com.gojek.mc2pinot.core.partition;

import org.junit.jupiter.api.Test;

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
}

