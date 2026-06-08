package com.consumer;

import com.consumer.application.AdaptiveConfig;
import com.consumer.application.ControllerPI;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        int     sampleIntervalSecs  = Integer.parseInt(appConfig.getProperty("monitor.sampleIntervalSeconds"));
        boolean controllerEnabled   = Boolean.parseBoolean(appConfig.getProperty("controller.enabled", "true"));
        int     maxPollRecordsLimit = Integer.parseInt(appConfig.getProperty("consumer.maxPollRecordsLimit"));
        int     stepRecords       = Integer.parseInt(appConfig.getProperty("benchmark.stepRecords"));
        int     stepIntervalSecs  = Integer.parseInt(appConfig.getProperty("benchmark.stepIntervalSeconds"));

        int currentMaxPollRecords = Integer.parseInt(appConfig.getProperty("consumer.initialMaxPollRecords"));

        ReentrantLock logLock = new ReentrantLock();

        ControllerPI pi      = null;
        Monitor      monitor = null;

        double[] targetRates = Arrays.stream(appConfig.getProperty("controller.targetRate").split(","))
                .mapToDouble(s -> Double.parseDouble(s.trim()))
                .toArray();
        int targetRateIntervalSecs = Integer.parseInt(appConfig.getProperty("controller.targetRateIntervalSeconds"));

        if (controllerEnabled) {
            pi = new ControllerPI(
                    Double.parseDouble(appConfig.getProperty("controller.kp")),
                    Double.parseDouble(appConfig.getProperty("controller.ki")),
                    targetRates[0],
                    sampleIntervalSecs,
                    MIN_MAX_POLL_RECORDS,
                    maxPollRecordsLimit);

            monitor = new Monitor(
                    logPath,
                    sampleIntervalSecs,
                    Integer.parseInt(appConfig.getProperty("monitor.readIntervalSeconds")),
                    logLock);

            double u = pi.compute(0);
            currentMaxPollRecords = (int) Math.round(u);
        }

        AdaptiveConfig cfg = buildConfig(currentMaxPollRecords, fetchMaxBytes, fetchWaitMaxMs);

        System.out.printf("Consumer started. Monitor=%b%n", controllerEnabled);
        if (controllerEnabled)
            System.out.printf("Log: %s | log every %ds | sample every %ds%n", logPath, logIntervalSecs, sampleIntervalSecs);
        else
            System.out.printf("Log: %s | log every %ds | step -%d records every %ds%n",
                    logPath, logIntervalSecs, stepRecords, stepIntervalSecs);

        int    logNumber      = 0;
        int    batchMessages  = 0;
        Instant nextLogAt       = Instant.now().plusSeconds(logIntervalSecs);
        Instant nextStepAt      = Instant.now().plusSeconds(stepIntervalSecs);
        int     targetIndex     = 0;
        Instant nextTargetAt    = Instant.now().plusSeconds(targetRateIntervalSecs);

        ExecutorService executor = Executors.newCachedThreadPool();

        KafkaConsumer<String, String> consumer = buildConsumer(bootstrapServers, groupId, cfg);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            // ReceiveMessage / ProcessMessage (parallel per record)
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                List<Future<long[]>> futures = new ArrayList<>();
                for (ConsumerRecord<String, String> record : records) {
                    final String value = record.value();
                    futures.add(executor.submit(() -> {
                        Thread.sleep(100); // simulate 100ms processing per message
                        long bytes = value != null ? value.length() : 0;
                        return new long[]{1L, bytes};
                    }));
                }
                for (Future<long[]> f : futures) {
                    long[] result = f.get();
                    batchMessages += (int) result[0];
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            } catch (Exception e) {
                System.out.printf("[Consumer] Error: %s%n", e.getMessage());
            }

            Instant now = Instant.now();

            // ── Target rate cycling ───────────────────────────────────────────
            if (controllerEnabled && !now.isBefore(nextTargetAt)) {
                targetIndex = (targetIndex + 1) % targetRates.length;
                pi.setTargetRate(targetRates[targetIndex]);
                System.out.printf("[Controller] Target rate changed to %.0f msg/s%n", targetRates[targetIndex]);
                nextTargetAt = nextTargetAt.plusSeconds(targetRateIntervalSecs);
            }

            // ── Adaptive control: PI (monitor on) or step increment (monitor off) ──
            if (controllerEnabled) {
                Double observed = monitor.tryObserve();
                if (observed != null) {
                    double u = pi.compute(observed);
                    currentMaxPollRecords = (int) Math.round(u);
                    System.out.printf("[Consumer] PI - max.poll.records=%d%n", currentMaxPollRecords);

                    cfg = buildConfig(currentMaxPollRecords, fetchMaxBytes, fetchWaitMaxMs);
                    consumer.close();
                    consumer = buildConsumer(bootstrapServers, groupId, cfg);
                    consumer.subscribe(Collections.singletonList(topic));
                }
            } else if (!now.isBefore(nextStepAt)) {
                currentMaxPollRecords = Math.max(currentMaxPollRecords + stepRecords, MIN_MAX_POLL_RECORDS);
                System.out.printf("[Benchmark] Step - max.poll.records=%d%n", currentMaxPollRecords);

                cfg = buildConfig(currentMaxPollRecords, fetchMaxBytes, fetchWaitMaxMs);
                consumer.close();
                consumer = buildConsumer(bootstrapServers, groupId, cfg);
                consumer.subscribe(Collections.singletonList(topic));
                nextStepAt = nextStepAt.plusSeconds(stepIntervalSecs);
            }

            // Log timer
            if (!now.isBefore(nextLogAt)) {
                logNumber++;
                writeLogBatch(logPath, cfg.maxPollRecords, batchMessages,  targetRates[targetIndex],  logLock);
                System.out.printf("[Consumer] Log %d | messages=%d | max.poll.records=%d%n", logNumber, batchMessages, cfg.maxPollRecords);
                batchMessages = 0;
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

    private static void writeLogBatch(String path, int maxPollRecords,
                                       int messages, double setpoint, ReentrantLock logLock) throws IOException {
        String line = String.format("%s|messages=%d|setpoint=%d|max.poll.records=%d",
                Instant.now(), messages, (int) setpoint, maxPollRecords);

        logLock.lock();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, true))) {
            writer.write(line);
            writer.newLine();
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
