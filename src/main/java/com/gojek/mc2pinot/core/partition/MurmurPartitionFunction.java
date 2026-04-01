package com.gojek.mc2pinot.core.partition;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class MurmurPartitionFunction implements PartitionFunction {

    @Override
    public int partition(String value, int numPartitions) {
        int hash = Hashing.murmur3_32_fixed(0).hashString(value, StandardCharsets.UTF_8).asInt();
        return (hash & Integer.MAX_VALUE) % numPartitions;
    }
}

