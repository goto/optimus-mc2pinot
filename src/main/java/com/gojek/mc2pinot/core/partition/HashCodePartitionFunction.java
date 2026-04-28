package com.gojek.mc2pinot.core.partition;

public class HashCodePartitionFunction implements PartitionFunction {

    @Override
    public int partition(String value, int numPartitions) {
        return new org.apache.pinot.segment.spi.partition.HashCodePartitionFunction(numPartitions)
                .getPartition(value);
    }
}

