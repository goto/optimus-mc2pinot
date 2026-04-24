package com.gojek.mc2pinot.core.partition;

public class ModuloPartitionFunction implements PartitionFunction {

    @Override
    public int partition(String value, int numPartitions) {
        return new org.apache.pinot.segment.spi.partition.ModuloPartitionFunction(numPartitions)
                .getPartition(value);
    }
}

