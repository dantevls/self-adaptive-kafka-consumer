# System Architecture — Self-Adaptive Kafka Consumer

## Overview

The system has three running processes orchestrated by Docker Compose, plus two independent consumer implementations that can be run interchangeably:

- **Producer** — floods Kafka with maximum-size messages as fast as possible
- **Kafka Broker** — routes and stores messages (KRaft mode, no Zookeeper)
- **Consumer** — consumes messages in timed log windows; a Monitor inside it continuously
  adapts one Kafka parameter using a PI feedback controller or a step-decrement benchmark

Two consumer implementations share the same algorithm but differ in the controlled variable:

| Implementation | Language | Controlled variable | Config file |
|---|---|---|---|
| `DotnetApplication/` | .NET 8 | `max.poll.interval.ms` | `appsettings.json` |
| `JavaApplication/` | Java 17 | `max.poll.records` | `application.properties` |

---

## Process Map

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Docker network                                                              │
│                                                                              │
│  ┌─────────────────┐   adaptive-topic   ┌──────────────────────────────┐   │
│  │    PRODUCER     │ ─────────────────▶ │         KAFKA BROKER         │   │
│  │  (Producer/)    │                    │   confluentinc/cp-kafka:7.6.1│   │
│  └─────────────────┘                    │   KRaft (no Zookeeper)       │   │
│                                         │   port 9092 (internal)       │   │
│                                         │   port 29092 (host access)   │   │
│                                         └──────────────┬───────────────┘   │
│                                                        │                    │
│                                               consume  ▼                    │
│                                         ┌──────────────────────────────┐   │
│                                         │  CONSUMER (either impl.)     │   │
│                                         │                              │   │
│                                         │  ┌────────────────────────┐ │   │
│                                         │  │       Monitor          │ │   │
│                                         │  │  ┌──────────────────┐  │ │   │
│                                         │  │  │  ControllerPI    │  │ │   │
│                                         │  │  └──────────────────┘  │ │   │
│                                         │  └────────────────────────┘ │   │
│                                         │           │                  │   │
│                                         │           ▼                  │   │
│                                         │     consumer_log.txt         │   │
│                                         └──────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Producer (`Producer/`)

### Responsibility
Saturate the Kafka topic with the largest possible messages at the maximum possible rate.

### How it works
1. Reads configuration from `appsettings.json`
2. Computes `payloadSize = MaxMessageBytes (1 000 000) − FramingOverhead (96) = 999 904 bytes`
3. Pre-builds **one fixed string** of that size — reused for every message (no allocation per send)
4. Calls `producer.Produce()` in a tight loop — fire-and-forget, does not await each acknowledgement
5. If the internal buffer fills up (`Local_QueueFull`), calls `producer.Poll(100ms)` to drain callbacks

### Key configuration (`Producer/appsettings.json`)
| Key | Value | Meaning |
|---|---|---|
| `MaxMessageBytes` | 1 000 000 | Kafka's hard message size limit |
| `LingerMs` | 5 | Broker-side linger: wait up to 5 ms to fill a batch |
| `BatchSize` | 16 777 216 | 16 MB internal batch buffer |
| `QueueBufferingMaxMessages` | 100 000 | Max messages held in internal queue |
| `QueueBufferingMaxKbytes` | 2 097 151 | ~2 GB max total in-flight data |

---

## 2. Kafka Broker

### Configuration highlights (`docker-compose.yml`)
| Setting | Value |
|---|---|
| Mode | KRaft (no Zookeeper) |
| Topic | `adaptive-topic` (auto-created on first produce) |
| Internal listener | `kafka:9092` — used by Producer and Consumer containers |
| External listener | `localhost:29092` — used to connect from the host machine |
| Replication factor | 1 (single broker) |

---

## 3. Consumer — Shared Algorithm

Both implementations follow the same loop structure. They differ only in which Kafka parameter is tuned.

### Main loop (`Program.cs` / `Main.java`)

```
STARTUP
  Read config file
  IF controllerEnabled:
      Build ControllerPI(Kp, Ki, targetRate, dt)
      Build Monitor(logPath, sampleIntervalSecs, readIntervalSecs, logLock)

LOOP (repeats forever)
  records = consumer.poll(200 ms)

  [Java only] FOR each record:
      submit to ExecutorService → sleep(100ms) + accumulate bytes   ← parallel processing
  [Java only] await all futures; accumulate batchMessages, batchBytes

  [.NET only] FOR each record:
      queue.Add(entry)          ← messageId | bytes | currentControlledValue

  IF records not empty:
      consumer.commitSync()

  IF controllerEnabled:
      observed = monitor.tryObserve()   ← null unless sample interval elapsed
      IF observed != null:
          u = pi.compute(observed)      ← new value for controlled variable
          currentValue = clamp(u, MIN, LIMIT)
          rebuild consumer with new config

  ELSE IF benchmark mode AND stepInterval elapsed:
      [.NET] currentValue -= stepAmount        ← blind decrement
      [Java] currentValue += stepAmount        ← blind increment (ramps up from MIN)
      currentValue = clamp(currentValue, MIN, LIMIT)
      rebuild consumer with new config
      advance nextStepAt

  IF logInterval elapsed:
      logNumber++
      writeLogBatch(logPath, logNumber, currentValue, queue/batch)
      queue.clear() / reset batchMessages & batchBytes
      advance nextLogAt
```

The consumer is **rebuilt** (closed + new instance) every time the controlled variable changes, because both `max.poll.interval.ms` and `max.poll.records` are set at construction time in their respective clients.

---

## 4. DotnetApplication (`DotnetApplication/`)

### Controlled variable
> **`max.poll.interval.ms`** — maximum time between two consecutive `Consume()` calls before the broker considers the consumer dead and triggers a rebalance. Lowering it increases pressure on the consumer to process fast.

### Files
| File | Role |
|---|---|
| `Program.cs` | Main loop — orchestrates consumer, adaptive control, log timer |
| `Monitor.cs` | Background thread — reads log file, computes rec/s, exposes `TryObserve()` |
| `ControllerPI.cs` | Pure PI math — `Compute(measuredRate)` → new `max.poll.interval.ms` |
| `SharedConfig.cs` | Thread-safe versioned config snapshot |
| `appsettings.json` | All runtime parameters |

### Benchmark mode (default, `Monitor:Enabled = false`)
```
Every StepIntervalSeconds (30 s):
    max.poll.interval.ms -= StepMs (1 000)
    clamped to [SessionTimeoutMs=6 000, InitialMaxPollIntervalMs=10 000]
```

### Monitor mode (`Monitor:Enabled = true`)
```
Every PiIntervalSeconds (60 s):
    y = records_consumed / piInterval   (rec/s, read from log file)
    u = ControllerPI.Compute(y)
    max.poll.interval.ms = clamp(u, 6 000, MaxPollIntervalMsLimit)
```

### Configuration (`DotnetApplication/appsettings.json`)
```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "Topic":            "adaptive-topic",
    "GroupId":          "adaptive-consumer-group"
  },
  "Consumer": {
    "LogPath":                  "…/logs/consumer_log.txt",
    "InitialMaxPollIntervalMs": 10000,
    "MaxPollIntervalMsLimit":   1000,
    "FetchMaxBytes":            1048576,
    "FetchWaitMaxMs":           500,
    "LogIntervalSeconds":       30
  },
  "Monitor": {
    "Enabled":             false,
    "PiIntervalSeconds":   60,
    "ReadIntervalSeconds": 60,
    "Pi": { "Kp": 1.0, "Ki": 0.01, "TargetPollIntervalMs": 1000 }
  },
  "Benchmark": {
    "StepMs":              1000,
    "StepIntervalSeconds": 30
  }
}
```

### Fixed Kafka consumer settings (DotnetApplication)
| Parameter | Value |
|---|---|
| `session.timeout.ms` | 6 000 (minimum allowed; `max.poll.interval.ms` must be ≥ this) |
| `fetch.min.bytes` | 1 |
| `fetch.max.bytes` | 1 048 576 (1 MB) |
| `fetch.max.wait.ms` | 500 |
| `enable.auto.commit` | false |
| `auto.offset.reset` | earliest |

---

## 5. JavaApplication (`JavaApplication/`)

### Controlled variable
> **`max.poll.records`** — maximum number of records returned by a single `poll()` call. Lowering it reduces the batch size per poll, which directly limits throughput per iteration.

### Files
| File | Role |
|---|---|
| `Main.java` | Main loop — orchestrates consumer, parallel processing, adaptive control, log timer |
| `Monitor.java` | Daemon thread — reads log file, computes rec/s, exposes `tryObserve()`; shares a `ReentrantLock` with Main to coordinate file access |
| `ControllerPI.java` | Pure PI math — `compute(measuredRate)` → new `max.poll.records` |
| `AdaptiveConfig.java` | Immutable config snapshot passed to each new consumer instance |
| `application.properties` | All runtime parameters |

### Parallel message processing (Java only)
Each record returned by `poll()` is submitted to a **cached thread pool** (`Executors.newCachedThreadPool()`). Each task sleeps 100 ms (simulating per-message work) and returns `{count=1, bytes}`. The main thread awaits all futures before proceeding to the control and log steps.

### Benchmark mode (default, `controller.enabled = false`)
```
Every benchmark.stepIntervalSeconds (180 s):
    max.poll.records += benchmark.stepRecords (10)   ← ramps UP from initialMaxPollRecords=10
    clamped to [MIN_MAX_POLL_RECORDS=1, maxPollRecordsLimit=1000]
```

### Monitor / PI mode (`controller.enabled = true`)
```
Every monitor.sampleIntervalSeconds (180 s):
    y = messages_consumed / sampleInterval   (rec/s, read from log file)
    u = ControllerPI.compute(y)
    max.poll.records = clamp(round(u), 1, maxPollRecordsLimit)
```

### Configuration (`JavaApplication/src/main/resources/application.properties`)
```properties
kafka.bootstrapServers=localhost:9092
kafka.topic=adaptive-topic
kafka.groupId=adaptive-consumer-group

consumer.logPath=…/logs/consumer_log.txt
consumer.initialMaxPollRecords=10
consumer.maxPollRecordsLimit=1000
consumer.fetchMaxBytes=1048576
consumer.fetchWaitMaxMs=500
consumer.logIntervalSeconds=1

monitor.sampleIntervalSeconds=180
monitor.readIntervalSeconds=180

controller.enabled=true
controller.kp=1.0
controller.ki=0
controller.targetRate=<target rec/s, e.g. 300 | 500 | 700>

benchmark.stepRecords=10
benchmark.stepIntervalSeconds=180
```

### Fixed Kafka consumer settings (JavaApplication)
| Parameter | Value |
|---|---|
| `session.timeout.ms` | 6 000 |
| `fetch.min.bytes` | 1 048 576 (1 MB, hardcoded) |
| `fetch.max.bytes` | 1 048 576 (from config) |
| `fetch.max.wait.ms` | 500 |
| `enable.auto.commit` | false |
| `auto.offset.reset` | earliest |

---

## 6. Monitor (shared logic, both implementations)

The Monitor runs on a **background thread** (daemon thread in Java, `Task.Run` in .NET).

In Java, Monitor receives the same `ReentrantLock` held by the log writer in Main so that file reads and writes never overlap.

```
LOOP (repeats forever)
  sleep(readIntervalSeconds)

  current = countLogRecords(logPath)
      [Java]  acquire logLock; sum messages= field from each summary line; release
      [.NET]  count non-header, non-blank lines

  IF now >= nextSampleAt:
      m = current − totalRecordsSeen   ← records since last sample tick
      y = m / sampleIntervalSeconds    ← rate in rec/s
      totalRecordsSeen = current
      pendingRate = y                  ← made available via tryObserve()
      nextSampleAt += sampleInterval
      print "[Monitor] m=… | y=… rec/s"
```

`tryObserve()` is called on the consumer thread every loop iteration. It returns `y` once per sample interval (then resets to null), so the consumer thread is never blocked.

---

## 7. ControllerPI (shared logic, both implementations)

Pure math. No knowledge of Kafka, files, or threads.

```
INPUT:   measuredRate   (y, in rec/s — already computed by Monitor)
         dt             (piIntervalSeconds, fixed at construction)

error    = target − measuredRate
P        = Kp  × error
I       += Ki  × error × dt          ← integral accumulates across calls
u        = P + I                     → returned as new controlled value
```

#### Tuning parameters (both implementations, default values)
| Parameter | Value | Meaning |
|---|---|---|
| `Kp` | 1.0 | Proportional gain |
| `Ki` (.NET) | 0.01 | Integral gain |
| `Ki` (Java) | 0 | Integral gain (pure-P by default) |
| `target` (.NET) | 1 000.0 | Target rate in rec/s (`TargetPollIntervalMs`) |
| `target` (Java) | configurable | Target throughput in rec/s (`controller.targetRate`) |
| `dt` (.NET) | 60 s | Sampling period (`piIntervalSeconds`) |
| `dt` (Java) | 180 s | Sampling period (`monitor.sampleIntervalSeconds`) |

---

## 8. Log File (`consumer_log.txt`)

Written by both implementations. Append-only. Same structure.

### DotnetApplication format
```
--- Log 1 | 2026-06-01T10:00:00Z | max.poll.interval.ms=10000 | messages=142 ---
0-100|999904|10000
0-101|999904|10000
...

--- Log 2 | 2026-06-01T10:00:30Z | max.poll.interval.ms=9000 | messages=137 ---
...
```

### JavaApplication format
```
2026-06-01T10:00:00Z|messages=1843731|bytes=1843522624|max.poll.records=10
2026-06-01T10:00:01Z|messages=1612044|bytes=1611920576|max.poll.records=10
...
```

Each line is one batch summary written every `logIntervalSeconds` (default 1 s). Fields: timestamp, total messages in the batch, total bytes, current `max.poll.records`.

The Monitor sums the `messages=` field across all lines to get the cumulative record count, which is used to compute the rec/s rate fed into the PI controller.

---

## 9. Data Flow

```
Producer                  Kafka Broker              Consumer (either impl.)
────────                  ────────────              ──────────────────────
payload = 999 904 B
Produce() ──────────────▶ adaptive-topic
Produce() ──────────────▶ [partition 0] ──────────▶ poll(200ms) → queue
Produce() ──────────────▶ [partition 0]             commitSync()
...                        (backlog builds)

                                                    [every logIntervalSecs]
                                                     writeLogBatch()
                                                          │
                                                          ▼
                                                    consumer_log.txt ◀── Monitor reads
                                                                              │
                                                                    y = rec/s (rec/piInterval)
                                                                              │
                                                                       ControllerPI
                                                                              │
                                                                         u = new value
                                                                              │
                                                              .NET: max.poll.interval.ms = clamp(u)
                                                             Java: max.poll.records      = clamp(u)
                                                                              │
                                                                    consumer.close()
                                                                    buildConsumer(newCfg)
                                                                    consumer.subscribe()
```

---

## 10. Separation of Concerns

| Class / File | Decides anything? | What it does |
|---|---|---|
| `Producer/Program.cs` | No | Produces at max rate using fixed config |
| `Program.cs` / `Main.java` | No | Runs the consume loop, logs, rebuilds consumer; Java: dispatches record processing to thread pool |
| `Monitor.cs` / `Monitor.java` | No | Measures rec/s from log file; exposes rate; Java: holds shared `ReentrantLock` with writer |
| `ControllerPI.cs` / `ControllerPI.java` | **YES** | Computes PI output → new controlled value |
| `appsettings.json` / `application.properties` | No | Stores all tunable parameters |
| `docker-compose.yml` | No | Wires containers, overrides bootstrap address |
