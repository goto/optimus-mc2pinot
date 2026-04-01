package com.gojek.mc2pinot.core.partition;

public class ModuloPartitionFunction implements PartitionFunction {

    @Override
    public int partition(String value, int numPartitions) {
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "ModuloPartitionFunction requires a numeric column value, got: " + value, e);
        }
        return (int) (Math.abs(parsed) % numPartitions);
    }
}

