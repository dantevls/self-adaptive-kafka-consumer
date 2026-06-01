namespace Consumer.Application;

/// <summary>
/// Pure PI controller. Receives the measured rate y directly (already computed by Monitor).
/// The sampling period d is fixed at construction time, matching Initialise(p1,...,pn) in Algorithm 1.
///
///   error  = target - y
///   P      = Kp * error
///   I     += Ki * error * d
///   u      = P + I          (returned to caller → SetMaxPollRecords)
/// </summary>
public sealed class ControllerPI
{
    private readonly double _kp;
    private readonly double _ki;
    private readonly double _targetPollIntervalMs;
    private readonly double _dt;

    private double _integral;

    /// <param name="dt">Sampling period in seconds (d in Algorithm 1).</param>
    public ControllerPI(double kp, double ki, double targetPollIntervalMs, double dt)
    {
        _kp                  = kp;
        _ki                  = ki;
        _targetPollIntervalMs = targetPollIntervalMs;
        _dt                  = dt;
    }

    /// <summary>Update(y) from Algorithm 1 — receives the measured interval, returns control output u.</summary>
    /// <param name="measuredRate">y = CalculateRate(m, d) in rec/s, already computed by Monitor.</param>
    public double Compute(double measuredRate)
    {
        double error = _targetPollIntervalMs - measuredRate;

        _integral += _ki * error * _dt;

        double u = _kp * error + _integral;

        Console.WriteLine(
            $"[PI] y={measuredRate:F2}  target={_targetPollIntervalMs:F0}  " +
            $"error={error:F2}  P={_kp * error:F2}  I={_integral:F2}  u={u:F2}");

        return u;
    }
}
