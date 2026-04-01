package com.gojek.mc2pinot;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    @Override
    public String format(LogRecord record) {
        String timestamp = TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(record.getMillis()));
        return timestamp + " " + record.getLevel().getName() + ": " + record.getMessage() + System.lineSeparator();
    }
}

