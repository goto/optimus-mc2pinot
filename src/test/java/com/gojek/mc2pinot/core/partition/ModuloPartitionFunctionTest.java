package com.gojek.mc2pinot.core.partition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ModuloPartitionFunctionTest {

    private final PartitionFunction function = new ModuloPartitionFunction();

    @Test
    void shouldReturnZeroForSinglePartition() {
        assertEquals(0, function.partition("42", 1));
    }

    @Test
    void shouldApplyModuloCorrectly() {
        assertEquals(2, function.partition("12", 5));
        assertEquals(0, function.partition("10", 5));
        assertEquals(3, function.partition("8", 5));
    }

    @Test
    void shouldHandleNegativeValues() {
        int result = function.partition("-7", 5);
        assertTrue(result >= 0 && result < 5);
    }

    @Test
    void shouldHandleZero() {
        assertEquals(0, function.partition("0", 4));
    }

    @Test
    void shouldThrowForNonNumericInput() {
        assertThrows(IllegalArgumentException.class, () -> function.partition("notANumber", 4));
    }

    @Test
    void shouldThrowForDecimalInput() {
        assertThrows(IllegalArgumentException.class, () -> function.partition("3.14", 4));
    }
}

