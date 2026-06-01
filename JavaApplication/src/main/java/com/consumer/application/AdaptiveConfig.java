package com.consumer.application;

public final class AdaptiveConfig {
    public final int maxPollRecords;
    public final int fetchMaxBytes;
    public final int fetchWaitMaxMs;

    public AdaptiveConfig(int maxPollRecords, int fetchMaxBytes, int fetchWaitMaxMs) {
        this.maxPollRecords = maxPollRecords;
        this.fetchMaxBytes  = fetchMaxBytes;
        this.fetchWaitMaxMs = fetchWaitMaxMs;
    }
}
