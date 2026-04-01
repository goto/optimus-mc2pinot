package com.gojek.mc2pinot.core.partition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MurmurPartitionFunctionTest {

    private final PartitionFunction function = new MurmurPartitionFunction();

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
    void shouldDifferFromHashCodeForSomeInputs() {
        PartitionFunction hashCode = new HashCodePartitionFunction();
        boolean differs = false;
        String[] samples = {"apple", "banana", "cherry", "date", "elderberry"};
        for (String s : samples) {
            if (function.partition(s, 100) != hashCode.partition(s, 100)) {
                differs = true;
                break;
            }
        }
        assertTrue(differs, "Murmur and HashCode should produce different results for some inputs");
    }
}

