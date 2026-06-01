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
 * When the PI interval elapses, computes y = m/d (records/s) and makes it
 * available via tryObserve(). All file I/O is off the consumer thread.
 */
public final class Monitor {
    private final String       logPath;
    private final Duration     piInterval;
    private final Duration     readInterval;
    private final ReentrantLock logLock;

    private long    totalRecordsSeen;
    private Instant nextPiAt;
    private Double  pendingRate;
    private final Object sync = new Object();

    public Monitor(String logPath, int piIntervalSeconds, int readIntervalSeconds, ReentrantLock logLock) {
        this.logPath      = logPath;
        this.piInterval   = Duration.ofSeconds(piIntervalSeconds);
        this.readInterval = Duration.ofSeconds(readIntervalSeconds);
        this.logLock      = logLock;
        this.nextPiAt     = Instant.now().plus(piInterval);

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
                    if (!now.isBefore(nextPiAt)) {
                        long   m = current - totalRecordsSeen;
                        double y = m / (double) piInterval.getSeconds();
                        totalRecordsSeen = current;
                        pendingRate      = y;
                        nextPiAt         = nextPiAt.plus(piInterval);
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
     * Returns y (rec/s) if the PI interval elapsed since the last call; null otherwise.
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
            long count = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("--- ")) continue;
                count++;
            }
            return count;
        } finally {
            logLock.unlock();
        }
    }
}
