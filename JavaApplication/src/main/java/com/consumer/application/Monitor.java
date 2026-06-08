package com.consumer.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs a daemon loop that reads the log file every readIntervalSeconds.
 * When the sample interval elapses, computes y = m/d (records/s) and makes it
 * available via tryObserve(). All file I/O is off the consumer thread.
 */
public final class Monitor {
    private final String        logPath;
    private final Duration      sampleInterval;
    private final Duration      readInterval;
    private final ReentrantLock logLock;

    private long    totalRecordsSeen;
    private Instant nextSampleAt;
    private Double  pendingRate;
    private final Object sync = new Object();

    public Monitor(String logPath, int sampleIntervalSeconds, int readIntervalSeconds, ReentrantLock logLock) {
        this.logPath        = logPath;
        this.sampleInterval = Duration.ofSeconds(sampleIntervalSeconds);
        this.readInterval   = Duration.ofSeconds(readIntervalSeconds);
        this.logLock        = logLock;
        this.nextSampleAt   = Instant.now().plus(sampleInterval);

        // warmup: snapshot current file state so the first measurement
        // only counts messages produced AFTER the controller starts
        try { this.totalRecordsSeen = countLogRecords(); }
        catch (Exception ignored) {}

        Thread t = new Thread(this::runLoop, "monitor-thread");
        t.setDaemon(true);
        t.start();
    }

    private void runLoop() {
        while (true) {
            try {
                Thread.sleep(readInterval.toMillis());

                long current = countLogRecords();
                Instant now  = Instant.now();

                synchronized (sync) {
                    if (!now.isBefore(nextSampleAt)) {
                        long   m = current - totalRecordsSeen;
                        double y = m / (double) sampleInterval.getSeconds();
                        totalRecordsSeen = current;
                        pendingRate      = y;
                        nextSampleAt     = nextSampleAt.plus(sampleInterval);
                        System.out.printf("[Monitor] m=%d records | y=%.2f rec/s%n", m, y);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * Returns y (rec/s) if the sample interval elapsed since the last call; null otherwise.
     * Non-blocking — safe to call on every consumer iteration.
     */
    public Double tryObserve() {
        synchronized (sync) {
            Double rate = pendingRate;
            pendingRate = null;
            return rate;
        }
    }

    private long countLogRecords() throws IOException {
        File f = new File(logPath);
        if (!f.exists()) return 0;

        logLock.lock();
        try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
            long total = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                for (String part : line.split("\\|")) {
                    if (part.startsWith("messages=")) {
                        total += Long.parseLong(part.substring("messages=".length()));
                        break;
                    }
                }
            }
            return total;
        } finally {
            logLock.unlock();
        }
    }
}
