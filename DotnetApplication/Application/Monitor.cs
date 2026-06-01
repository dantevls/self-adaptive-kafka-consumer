namespace Consumer.Application;

/// <summary>
/// Runs a background loop that reads the log file every ReadIntervalSeconds.
/// When the PI interval elapses, computes y = m/d (records/s) and makes it available via TryObserve().
/// All file I/O happens off the consumer thread.
/// </summary>
public sealed class Monitor
{
    private readonly string _logPath;
    private readonly TimeSpan _piInterval;
    private readonly TimeSpan _readInterval;
    private readonly SemaphoreSlim _logLock;

    private long _totalRecordsSeen;
    private DateTime _nextPiAt;
    private double? _pendingRate;
    private readonly object _sync = new();

    public Monitor(string logPath, int piIntervalSeconds, int readIntervalSeconds, SemaphoreSlim logLock)
    {
        _logPath = logPath;
        _piInterval = TimeSpan.FromSeconds(piIntervalSeconds);
        _readInterval = TimeSpan.FromSeconds(readIntervalSeconds);
        _logLock = logLock;
        _nextPiAt = DateTime.UtcNow.Add(_piInterval);

        Task.Run(RunAsync);
    }

    private async Task RunAsync()
    {
        while (true)
        {
            try
            {
                await Task.Delay(_readInterval);

                // File I/O outside the lock so the consumer thread is never blocked.
                long current = await CountLogRecordsAsync();
                var now = DateTime.UtcNow;

                lock (_sync)
                {
                    if (now >= _nextPiAt)
                    {
                        long   m = current - _totalRecordsSeen;
                        double y = m / _piInterval.TotalSeconds;   // CalculateRate(m, d) in rec/s
                        _totalRecordsSeen = current;
                        _pendingRate      = y;
                        _nextPiAt         = _nextPiAt.Add(_piInterval);
                        Console.WriteLine($"[Monitor] m={m} records | y={y:F2} rec/s");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message} | {ex.StackTrace}");
            }
        }
    }

    /// <summary>
    /// Returns y (rec/s) if the PI interval elapsed since the last call; null otherwise.
    /// Non-blocking — safe to call on every consumer iteration.
    /// </summary>
    public double? TryObserve()
    {
        lock (_sync)
        {
            var rate = _pendingRate;
            _pendingRate = null;
            return rate;
        }
    }

    private async Task<long> CountLogRecordsAsync()
    {
        if (!File.Exists(_logPath)) return 0;

        await _logLock.WaitAsync();
        try
        {
            long count = 0;
            foreach (var line in File.ReadLines(_logPath))
            {
                if (string.IsNullOrWhiteSpace(line) || line.StartsWith("--- ")) continue;
                count++;
            }
            return count;
        }
        finally
        {
            _logLock.Release();
        }
    }
}

public sealed class AdaptiveConfig
{
    public int MaxPollIntervalMs { get; init; }
    public int FetchMaxBytes     { get; init; }
    public int FetchWaitMaxMs    { get; init; }
}
