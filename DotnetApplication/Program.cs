using System.Globalization;
using Confluent.Kafka;
using Consumer.Application;
using Microsoft.Extensions.Configuration;

var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()   // env vars override appsettings (used by Docker)
    .Build();

var bootstrapServers         = config["Kafka:BootstrapServers"]!;
var topic                    = config["Kafka:Topic"]!;
var groupId                  = config["Kafka:GroupId"]!;
var logPath                  = config["Consumer:LogPath"]!;
var fetchMaxBytes            = int.Parse(config["Consumer:FetchMaxBytes"]!);
var fetchWaitMaxMs           = int.Parse(config["Consumer:FetchWaitMaxMs"]!);
var logIntervalSeconds       = int.Parse(config["Consumer:LogIntervalSeconds"]!);
var piIntervalSeconds        = int.Parse(config["Monitor:PiIntervalSeconds"]!);
var monitorEnabled           = bool.Parse(config["Monitor:Enabled"] ?? "true");
var maxPollIntervalMsLimit   = int.Parse(config["Consumer:MaxPollIntervalMsLimit"]!);
var stepMs                   = int.Parse(config["Benchmark:StepMs"]!);
var stepIntervalSeconds      = int.Parse(config["Benchmark:StepIntervalSeconds"]!);

const int SessionTimeoutMs     = 6000;   // Kafka minimum; MaxPollIntervalMs must always be >= this
const int MinMaxPollIntervalMs = SessionTimeoutMs;
var currentMaxPollIntervalMs = int.Parse(config["Consumer:InitialMaxPollIntervalMs"]!);

var logLock = new SemaphoreSlim(1, 1);

// PI controller and Monitor are only created when enabled.
ControllerPI? pi = null;
Consumer.Application.Monitor? monitor = null;

if (monitorEnabled)
{
    pi = new ControllerPI(
        kp:                  double.Parse(config["Monitor:Pi:Kp"]!, CultureInfo.InvariantCulture),
        ki:                  double.Parse(config["Monitor:Pi:Ki"]!, CultureInfo.InvariantCulture),
        targetPollIntervalMs: double.Parse(config["Monitor:Pi:TargetPollIntervalMs"]!, CultureInfo.InvariantCulture),
        dt:                  piIntervalSeconds);

    monitor = new Consumer.Application.Monitor(
        logPath:             logPath,
        piIntervalSeconds:   piIntervalSeconds,
        readIntervalSeconds: int.Parse(config["Monitor:ReadIntervalSeconds"]!),
        logLock:             logLock);
}

var cfg = BuildConfig(currentMaxPollIntervalMs, fetchMaxBytes, fetchWaitMaxMs);

Console.WriteLine($"Consumer started. Monitor={monitorEnabled}");
if (monitorEnabled)
    Console.WriteLine($"Log: {logPath} | log every {logIntervalSeconds}s | PI every {piIntervalSeconds}s");
else
    Console.WriteLine($"Log: {logPath} | log every {logIntervalSeconds}s | step +{stepMs}ms every {stepIntervalSeconds}s");

var logNumber = 0;
var nextLogAt  = DateTime.UtcNow.AddSeconds(logIntervalSeconds);
var nextStepAt = DateTime.UtcNow.AddSeconds(stepIntervalSeconds);
var queue      = new List<LogEntry>();

var consumer = BuildConsumer(bootstrapServers, groupId, cfg);
consumer.Subscribe(topic);

while (true)
{
    // ── ReceiveMessage / ProcessMessage ───────────────────────────────────────
    try
    {
        var result = consumer.Consume(TimeSpan.FromMilliseconds(200));
        if (result is not null)
        {
            consumer.Commit(result);
            queue.Add(new LogEntry(
                MessageId:          $"{result.Partition.Value}-{result.Offset.Value}",
                Bytes:              result.Message.Value.Length,
                MaxPollIntervalMs:  cfg.MaxPollIntervalMs));
        }
    }
    catch (ConsumeException ex)
    {
        Console.WriteLine($"[Consumer] Error: {ex.Error.Reason}");
    }

    var now = DateTime.UtcNow;

    // ── Adaptive control: PI (monitor on) or step increment (monitor off) ────
    if (monitorEnabled)
    {
        var observed = monitor!.TryObserve();
        if (observed.HasValue)
        {
            double u = pi!.Compute(observed.Value);
            currentMaxPollIntervalMs = (int)Math.Clamp(u, MinMaxPollIntervalMs, maxPollIntervalMsLimit);
            Console.WriteLine($"[Consumer] PI → max.poll.interval.ms={currentMaxPollIntervalMs}");

            cfg = BuildConfig(currentMaxPollIntervalMs, fetchMaxBytes, fetchWaitMaxMs);
            consumer.Close();
            consumer = BuildConsumer(bootstrapServers, groupId, cfg);
            consumer.Subscribe(topic);
        }
    }
    else if (now >= nextStepAt)
    {
        currentMaxPollIntervalMs = Math.Max(currentMaxPollIntervalMs - stepMs, MinMaxPollIntervalMs);
        Console.WriteLine($"[Benchmark] Step → max.poll.interval.ms={currentMaxPollIntervalMs}");

        cfg = BuildConfig(currentMaxPollIntervalMs, fetchMaxBytes, fetchWaitMaxMs);
        consumer.Close();
        consumer = BuildConsumer(bootstrapServers, groupId, cfg);
        consumer.Subscribe(topic);
        nextStepAt = nextStepAt.AddSeconds(stepIntervalSeconds);
    }

    // ── Log timer (LogIntervalSeconds) ────────────────────────────────────────
    if (now >= nextLogAt)
    {
        logNumber++;
        await WriteLogBatch(logPath, logNumber, cfg.MaxPollIntervalMs, queue, logLock);
        Console.WriteLine($"[Consumer] Log {logNumber} — {queue.Count} messages");
        queue.Clear();
        nextLogAt = nextLogAt.AddSeconds(logIntervalSeconds);
    }
}

static AdaptiveConfig BuildConfig(int maxPollIntervalMs, int fetchMaxBytes, int fetchWaitMaxMs) => new()
{
    MaxPollIntervalMs = maxPollIntervalMs,
    FetchMaxBytes     = fetchMaxBytes,
    FetchWaitMaxMs    = fetchWaitMaxMs,
};

static IConsumer<Ignore, string> BuildConsumer(string bootstrap, string group, AdaptiveConfig cfg) =>
    new ConsumerBuilder<Ignore, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrap,
        GroupId          = group,
        AutoOffsetReset  = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        SessionTimeoutMs = SessionTimeoutMs,
        FetchMinBytes    = 1,
        FetchMaxBytes    = cfg.FetchMaxBytes,
        FetchWaitMaxMs   = cfg.FetchWaitMaxMs,
        MaxPollIntervalMs = cfg.MaxPollIntervalMs,
    }).Build();

static async Task WriteLogBatch(string path, int logNumber, int maxPollIntervalMs, List<LogEntry> entries, SemaphoreSlim logLock)
{
    var lines = new List<string>
    {
        $"--- Log {logNumber} | {DateTime.UtcNow:O} | max.poll.interval.ms={maxPollIntervalMs} | messages={entries.Count} ---"
    };
    lines.AddRange(entries.Select(e => $"{e.MessageId}|{e.Bytes}|{e.MaxPollIntervalMs}"));
    lines.Add(string.Empty);
    await logLock.WaitAsync();
    try
    {
        await File.AppendAllLinesAsync(path, lines);
    }
    finally
    {
        logLock.Release();
    }
}

record LogEntry(string MessageId, int Bytes, int MaxPollIntervalMs);
