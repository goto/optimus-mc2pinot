package com.gojek.mc2pinot.core.partition;

public class HashCodePartitionFunction implements PartitionFunction {

    @Override
    public int partition(String value, int numPartitions) {
        int h = value.hashCode();
        return (h == Integer.MIN_VALUE ? 0 : Math.abs(h)) % numPartitions;
    }
}

