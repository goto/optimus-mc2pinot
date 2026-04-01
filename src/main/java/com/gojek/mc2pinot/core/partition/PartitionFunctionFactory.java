package com.gojek.mc2pinot.core.partition;

import java.util.logging.Logger;

public class PartitionFunctionFactory {

    private static final Logger LOG = Logger.getLogger(PartitionFunctionFactory.class.getName());

    private PartitionFunctionFactory() {
    }

    public static PartitionFunction create(String functionName) {
        String name = (functionName == null || functionName.isBlank()) ? "hashcode" : functionName.toLowerCase();
        LOG.info("transient(core): create partition function " + name);
        return switch (name) {
            case "hashcode" -> new HashCodePartitionFunction();
            case "murmur" -> new MurmurPartitionFunction();
            case "modulo" -> new ModuloPartitionFunction();
            default -> throw new UnsupportedOperationException("Unknown partition function: " + functionName);
        };
    }
}

