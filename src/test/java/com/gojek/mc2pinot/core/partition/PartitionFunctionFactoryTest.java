package com.gojek.mc2pinot.core.partition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PartitionFunctionFactoryTest {

    @Test
    void shouldCreateHashCodeForHashcodeName() {
        assertInstanceOf(HashCodePartitionFunction.class, PartitionFunctionFactory.create("hashcode"));
    }

    @Test
    void shouldCreateHashCodeForUpperCaseName() {
        assertInstanceOf(HashCodePartitionFunction.class, PartitionFunctionFactory.create("HASHCODE"));
    }

    @Test
    void shouldCreateHashCodeForNullName() {
        assertInstanceOf(HashCodePartitionFunction.class, PartitionFunctionFactory.create(null));
    }

    @Test
    void shouldCreateHashCodeForBlankName() {
        assertInstanceOf(HashCodePartitionFunction.class, PartitionFunctionFactory.create("   "));
    }

    @Test
    void shouldCreateMurmurForMurmurName() {
        assertInstanceOf(MurmurPartitionFunction.class, PartitionFunctionFactory.create("murmur"));
    }

    @Test
    void shouldCreateModuloForModuloName() {
        assertInstanceOf(ModuloPartitionFunction.class, PartitionFunctionFactory.create("modulo"));
    }

    @Test
    void shouldThrowForUnknownFunctionName() {
        assertThrows(UnsupportedOperationException.class, () -> PartitionFunctionFactory.create("unknown"));
    }
}

