package com.consumer.application;

public final class LogEntry {
    public final String messageId;
    public final int    bytes;
    public final int    maxPollRecords;

    public LogEntry(String messageId, int bytes, int maxPollRecords) {
        this.messageId      = messageId;
        this.bytes          = bytes;
        this.maxPollRecords = maxPollRecords;
    }
}
