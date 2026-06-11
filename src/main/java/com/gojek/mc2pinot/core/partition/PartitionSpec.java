package com.gojek.mc2pinot.core.partition;

public record PartitionSpec(String column, int count) {}