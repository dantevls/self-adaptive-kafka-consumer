package com.consumer;

import com.consumer.application.AdaptiveConfig;
import com.consumer.application.ControllerPI;
import com.consumer.application.LogEntry;
import com.consumer.application.Monitor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public class Main {

    private static final int SESSION_TIMEOUT_MS   = 6000;
    private static final int MIN_MAX_POLL_RECORDS = 1;

    public static void main(String[] args) throws Exception {
        Properties appConfig = loadProperties();

        String  bootstrapServers  = appConfig.getProperty("kafka.bootstrapServers");
        String  topic             = appConfig.getProperty("kafka.topic");
        String  groupId           = appConfig.getProperty("kafka.groupId");
        String  logPath           = appConfig.getProperty("consumer.logPath");
        int     fetchMaxBytes     = Integer.parseInt(appConfig.getProperty("consumer.fetchMaxBytes"));
        int     fetchWaitMaxMs    = Integer.parseInt(appConfig.getProperty("consumer.fetchWaitMaxMs"));
        int     logIntervalSecs   = Integer.parseInt(appConfig.getProperty("consumer.logIntervalSeconds"));
        int     piIntervalSecs    = Integer.parseInt(appConfig.getProperty("monitor.piIntervalSeconds"));
        boolean monitorEnabled    = Boolean.parseBoolean(appConfig.getProperty("monitor.enabled", "true"));
        int     maxPollRecordsLimit = Integer.parseInt(appConfig.getProperty("consumer.maxPollRecordsLimit"));
        int     stepRecords       = Integer.parseInt(appConfig.getProperty("benchmark.stepRecords"));
        int     stepIntervalSecs  = Integer.parseInt(appConfig.getProperty("benchmark.stepIntervalSeconds"));

        int currentMaxPollRecords = Integer.parseInt(appConfig.getProperty("consumer.initialMaxPollRecords"));

        ReentrantLock logLock = new ReentrantLock();

        ControllerPI pi      = null;
        Monitor      monitor = null;

        if (monitorEnabled) {
            pi = new ControllerPI(
                    Double.parseDouble(appConfig.getProperty("monitor.pi.kp")),
                    Double.parseDouble(appConfig.getProperty("monitor.pi.ki")),
                    Double.parseDouble(appConfig.getProperty("monitor.pi.targetPollRecords")),
                    piIntervalSecs);

            monitor = new Monitor(
                    logPath,
                    piIntervalSecs,
                    Integer.parseInt(appConfig.getProperty("monitor.readIntervalSeconds")),
                    logLock);
        }

        AdaptiveConfig cfg = buildConfig(currentMaxPollRecords, fetchMaxBytes, fetchWaitMaxMs);

        System.out.printf("Consumer started. Monitor=%b%n", monitorEnabled);
        if (monitorEnabled)
            System.out.printf("Log: %s | log every %ds | PI every %ds%n", logPath, logIntervalSecs, piIntervalSecs);
        else
            System.out.printf("Log: %s | log every %ds | step -%d records every %ds%n",
                    logPath, logIntervalSecs, stepRecords, stepIntervalSecs);

        int    logNumber  = 0;
        Instant nextLogAt  = Instant.now().plusSeconds(logIntervalSecs);
        Instant nextStepAt = Instant.now().plusSeconds(stepIntervalSecs);
        List<LogEntry> queue = new ArrayList<>();

        KafkaConsumer<String, String> consumer = buildConsumer(bootstrapServers, groupId, cfg);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            // ── ReceiveMessage / ProcessMessage ──────────────────────────────
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    queue.add(new LogEntry(
                            record.partition() + "-" + record.offset(),
                            record.value() != null ? record.value().length() : 0,
                            cfg.maxPollRecords));
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            } catch (Exception e) {
                System.out.printf("[Consumer] Error: %s%n", e.getMessage());
            }

            Instant now = Instant.now();

            // ── Adaptive control: PI (monitor on) or step decrement (monitor off)
            if (monitorEnabled) {
                Double observed = monitor.tryObserve();
                if (observed != null) {
                    double u = pi.compute(observed);
                    currentMaxPollRecords = (int) Math.max(MIN_MAX_POLL_RECORDS,
                            Math.min(maxPollRecordsLimit, Math.round(u)));
                    System.out.printf("[Consumer] PI → max.poll.records=%d%n", currentMaxPollRecords);

                    cfg = buildConfig(currentMaxPollRecords, fetchMaxBytes, fetchWaitMaxMs);
                    consumer.close();
                    consumer = buildConsumer(bootstrapServers, groupId, cfg);
                    consumer.subscribe(Collections.singletonList(topic));
                }
            } else if (!now.isBefore(nextStepAt)) {
                currentMaxPollRecords = Math.max(currentMaxPollRecords - stepRecords, MIN_MAX_POLL_RECORDS);
                System.out.printf("[Benchmark] Step → max.poll.records=%d%n", currentMaxPollRecords);

                cfg = buildConfig(currentMaxPollRecords, fetchMaxBytes, fetchWaitMaxMs);
                consumer.close();
                consumer = buildConsumer(bootstrapServers, groupId, cfg);
                consumer.subscribe(Collections.singletonList(topic));
                nextStepAt = nextStepAt.plusSeconds(stepIntervalSecs);
            }

            // ── Log timer ─────────────────────────────────────────────────────
            if (!now.isBefore(nextLogAt)) {
                logNumber++;
                writeLogBatch(logPath, logNumber, cfg.maxPollRecords, queue, logLock);
                System.out.printf("[Consumer] Log %d — %d messages%n", logNumber, queue.size());
                queue.clear();
                nextLogAt = nextLogAt.plusSeconds(logIntervalSecs);
            }
        }
    }

    private static AdaptiveConfig buildConfig(int maxPollRecords, int fetchMaxBytes, int fetchWaitMaxMs) {
        return new AdaptiveConfig(maxPollRecords, fetchMaxBytes, fetchWaitMaxMs);
    }

    private static KafkaConsumer<String, String> buildConsumer(String bootstrap, String groupId, AdaptiveConfig cfg) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       String.valueOf(SESSION_TIMEOUT_MS));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,          "1048576");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,          String.valueOf(cfg.fetchMaxBytes));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,        String.valueOf(cfg.fetchWaitMaxMs));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         String.valueOf(cfg.maxPollRecords));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private static void writeLogBatch(String path, int logNumber, int maxPollRecords,
                                       List<LogEntry> entries, ReentrantLock logLock) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add(String.format("--- Log %d | %s | max.poll.records=%d | messages=%d ---",
                logNumber, Instant.now(), maxPollRecords, entries.size()));
        for (LogEntry e : entries) {
            lines.add(e.messageId + "|" + e.bytes + "|" + e.maxPollRecords);
        }
        lines.add("");

        logLock.lock();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, true))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        } finally {
            logLock.unlock();
        }
    }

    private static Properties loadProperties() throws IOException {
        Properties props = new Properties();
        // Prefer classpath (packaged JAR), fall back to working directory
        try (InputStream is = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
                return props;
            }
        }
        try (InputStream is = new FileInputStream("application.properties")) {
            props.load(is);
        }
        return props;
    }
}
