# System Architecture — Kafka Adaptive System (.NET)

## Overview

The system has three running processes orchestrated by Docker Compose:
- **Producer** — floods Kafka with maximum-size messages as fast as possible
- **Kafka Broker** — routes and stores messages (KRaft mode, no Zookeeper)
- **Consumer** — consumes messages in timed windows; a Monitor inside it continuously
  adapts the Kafka fetch configuration using two independent rules

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
│                                                        ▼ consume            │
│                                         ┌──────────────────────────────┐   │
│                                         │         CONSUMER             │   │
│                                         │        (Consumer/)           │   │
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
4. Calls `producer.Produce()` in a tight loop — **fire-and-forget**, does not await each acknowledgement
5. If the internal buffer fills up (`Local_QueueFull`), calls `producer.Poll(100ms)` to drain
   pending delivery callbacks and then continues

### Key configuration (`Producer/appsettings.json`)
| Key | Value | Meaning |
|---|---|---|
| `MaxMessageBytes` | 1 000 000 | Kafka's hard message size limit |
| `LingerMs` | 5 | Broker-side linger: wait up to 5 ms to fill a batch |
| `BatchSize` | 16 777 216 | 16 MB internal batch buffer |
| `QueueBufferingMaxMessages` | 100 000 | Max messages held in internal queue |
| `QueueBufferingMaxKbytes` | 2 097 151 | ~2 GB max total in-flight data |

### What does NOT exist here
- No logging to file
- No delay between sends
- No knowledge of the consumer or the Monitor

---

## 2. Kafka Broker

### Responsibility
Receive messages from the Producer, store them durably, and serve them to the Consumer on demand.

### Configuration highlights (`docker-compose.yml`)
| Setting | Value |
|---|---|
| Mode | KRaft (no Zookeeper needed) |
| Topic | `adaptive-topic` (auto-created on first produce) |
| Internal listener | `kafka:9092` — used by Producer and Consumer containers |
| External listener | `localhost:29092` — used to connect from the host machine |
| Replication factor | 1 (single broker) |

The broker is passive — it applies no logic. It simply buffers messages until the Consumer fetches them.

---

## 3. Consumer (`Consumer/`)

### Responsibility
Consume messages in fixed-duration windows, log each window to a file, and use the Monitor to
continuously adjust the Kafka fetch configuration.

### Files
| File | Role |
|---|---|
| `Program.cs` | Main loop — passive orchestrator |
| `Monitor.cs` | ALL adaptive logic — owns every tuning decision |
| `ControllerPI.cs` | Pure PI math — called by Monitor only |
| `appsettings.json` | All parameters for Consumer + Monitor |

---

### 3a. Program.cs — Main loop

```
STARTUP
  Read appsettings.json
  Build ControllerPI  (Kp, Ki, target)
  Build Monitor       (all parameters + PI instance)
  cfg = Monitor.Initial()          ← first config, no adaptation yet

LOOP (repeats forever)
  windowNumber++
  Build Kafka consumer using cfg   ← passive: just applies what Monitor decided
  Subscribe to topic

  START stopwatch (WindowSeconds = 60 s)
  WHILE time remaining > 0:
      result = consumer.Consume(200 ms timeout)
      IF message received:
          totalBytes += message size
          consumer.Commit(result)  ← manual commit, per message
          queue.Add(entry)         ← store: messageId | bytes | fetch.min.bytes

  consumer.Close()

  WriteMinuteBatch(logPath, window, fetch.min.bytes, queue)
  ← appends one block to consumer_log.txt

  cfg = Monitor.Next(totalBytes)   ← Monitor decides next fetch.min.bytes
```

The loop has **zero decision logic**. It only executes what Monitor returns.

---

### 3b. Monitor.cs — Adaptive logic

The Monitor owns one variable:

> **`fetch.min.bytes`** — tells the Kafka broker: "do not reply to a fetch request
> until you have at least N bytes ready." Larger values = fewer, larger batches.

It adjusts this variable using **two independent rules**, each on its own cadence:

#### Rule 1 — Fixed Increment (open-loop exploration)
```
Fires every:  IncrementIntervalWindows = 1  → every window (every 60 s)
Action:       fetch.min.bytes += IncrementBytes (1 024)
              clamped to [1, FetchMaxBytes (52 428 800)]
```
This rule is blind — it always pushes `fetch.min.bytes` upward regardless of what is
happening. It is the exploration mechanism: the system constantly probes whether a higher
threshold produces better throughput.

#### Rule 2 — PI Controller (closed-loop correction)
```
Fires every:  PiIntervalWindows = 3  → every 3rd window (every 3 min)
Input:        totalBytes consumed in the last window
Action:       calls ControllerPI.Compute(totalBytes, dt)
              applies the returned byte delta to fetch.min.bytes
              clamped to [1, FetchMaxBytes (52 428 800)]
```
This rule reacts to measured throughput. It corrects the trajectory toward the target.

#### Combined firing timeline
```
Window 1  (t=0 min)   Rule 1 fires              fetch.min.bytes += 1 024
Window 2  (t=1 min)   Rule 1 fires              fetch.min.bytes += 1 024
Window 3  (t=2 min)   Rule 1 + Rule 2 fire      fetch.min.bytes += 1 024 + PI delta
Window 4  (t=3 min)   Rule 1 fires              fetch.min.bytes += 1 024
Window 5  (t=4 min)   Rule 1 fires              fetch.min.bytes += 1 024
Window 6  (t=5 min)   Rule 1 + Rule 2 fire      fetch.min.bytes += 1 024 + PI delta
...
```

---

### 3c. ControllerPI.cs — PI Math

Pure math class. Has no knowledge of Kafka, windows, or files.

```
INPUT:   bytesProcessed (total bytes in the last window)
         dt             (window duration in seconds)

measured = bytesProcessed / dt          (actual throughput in B/s)
error    = target - measured            (how far from goal)

P        = Kp  × error                 (proportional term)
I       += Ki  × error × dt            (integral term — accumulates over time)

output   = P + I                        (byte delta returned to Monitor)
```

#### Current tuning (`Consumer/appsettings.json`)
| Parameter | Value | Meaning |
|---|---|---|
| `Kp` | 200.0 | 200 bytes of adjustment per B/s of error |
| `Ki` | 2.0 | Integral gain — corrects persistent offset slowly |
| `TargetBytesPerSecond` | 6 553 600 | ~6.25 MB/s target throughput (~6.5 messages/s at 999 904 B each) |

#### Why PI and not PID?
The derivative term (D) amplifies measurement noise. Since `bytesProcessed` is a coarse
per-window sample (not a continuous signal), the derivative would react to normal window-to-window
variation and cause instability. PI is the right choice for this sampling frequency.

---

## 4. Log File (`consumer_log.txt`)

Written by the Consumer after each window. Append-only. Format:

```
--- Window 1 | 2026-05-20T10:00:00Z | fetch.min.bytes=1 | messages=6 ---
0-100|999904|1
0-101|999904|1
...

--- Window 2 | 2026-05-20T10:01:00Z | fetch.min.bytes=1025 | messages=8 ---
0-106|999904|1025
...
```

Each line inside a block: `partition-offset | bytes | fetch.min.bytes active during that window`

The log is the only persistent output of the system. It allows post-hoc analysis of how
`fetch.min.bytes` evolved and how throughput responded.

---

## 5. Configuration Summary

### Producer (`Producer/appsettings.json`)
```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "Topic":            "adaptive-topic",
    "MaxMessageBytes":  1000000,
    "LingerMs":         5,
    "BatchSize":        16777216,
    "QueueBufferingMaxMessages": 100000,
    "QueueBufferingMaxKbytes":   2097151
  }
}
```

### Consumer (`Consumer/appsettings.json`)
```json
{
  "Kafka":    { "BootstrapServers": "localhost:9092", "Topic": "adaptive-topic", "GroupId": "adaptive-consumer-group" },
  "Consumer": { "LogPath": "consumer_log.txt" },
  "Monitor": {
    "InitialFetchMinBytes":    1,
    "FetchMaxBytes":           52428800,
    "FetchWaitMaxMs":          1000,
    "WindowSeconds":           60,
    "IncrementBytes":          1024,
    "IncrementIntervalWindows": 1,
    "PiIntervalWindows":       3,
    "Pi": { "Kp": 200.0, "Ki": 2.0, "TargetBytesPerSecond": 6553600 }
  }
}
```

---

## 6. Data Flow Diagram

```
Producer                  Kafka Broker              Consumer
────────                  ────────────              ────────
payload = 999 904 B
Produce() ──────────────▶ adaptive-topic
Produce() ──────────────▶ [partition 0] ──────────▶ Consume() → queue
Produce() ──────────────▶ [partition 0]             Commit()
...                        (backlog builds)          ...
                                                     [60s window ends]
                                                     WriteMinuteBatch()
                                                          │
                                                          ▼
                                                    consumer_log.txt
                                                          │
                                                     Monitor.Next(totalBytes)
                                                          │
                                              ┌───────────┴────────────┐
                                              │                        │
                                         Rule 1                    Rule 2
                                       (+1 024 B)             ControllerPI
                                       every window           every 3 windows
                                              │                        │
                                              └───────────┬────────────┘
                                                          │
                                                   new fetch.min.bytes
                                                          │
                                                   new AdaptiveConfig
                                                          │
                                             next window uses this config
```

---

## 7. Separation of Concerns

| Class / File | Decides anything? | What it does |
|---|---|---|
| `Producer/Program.cs` | No | Produces at max rate using config values |
| `Consumer/Program.cs` | No | Runs windows, commits, logs, passes bytes to Monitor |
| `Monitor.cs` | **YES — all decisions** | Owns fetch.min.bytes, fires rules on schedule |
| `ControllerPI.cs` | No | Pure math: computes PI output from error |
| `appsettings.json` (both) | No | Stores all tunable parameters |
| `docker-compose.yml` | No | Wires containers together, overrides bootstrap address |
