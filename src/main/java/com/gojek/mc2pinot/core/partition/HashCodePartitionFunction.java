package com.gojek.mc2pinot.core.partition;

public class HashCodePartitionFunction implements PartitionFunction {

    @Override
    public int partition(String value, int numPartitions) {
        return (value.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

