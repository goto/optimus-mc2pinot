package com.gojek.mc2pinot.core.partition;

public interface PartitionFunction {
    int partition(String value, int numPartitions);
}

