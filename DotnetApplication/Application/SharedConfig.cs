namespace Consumer.Application;

/// <summary>
/// Thread-safe, versioned config object shared between Monitor (writer) and
/// the consume loop (reader). The consumer detects changes by comparing versions.
/// </summary>
public sealed class SharedConfig
{
    private readonly object _lock = new();
    private AdaptiveConfig  _config;
    private int             _version;

    public SharedConfig(AdaptiveConfig initial)
    {
        _config  = initial;
        _version = 0;
    }

    /// <summary>Returns a consistent snapshot of (config, version) under the lock.</summary>
    public (AdaptiveConfig Config, int Version) Snapshot
    {
        get { lock (_lock) { return (_config, _version); } }
    }

    /// <summary>Called only by Monitor. Increments version so the consumer reacts.</summary>
    public void Update(AdaptiveConfig newConfig)
    {
        lock (_lock)
        {
            _config = newConfig;
            _version++;
        }
    }
}
