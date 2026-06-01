package com.consumer.application;

/**
 * Pure PI controller. Receives measured rate y (rec/s) from Monitor.
 * The sampling period dt is fixed at construction time (Algorithm 1).
 *
 *   error  = targetPollRecords - y
 *   P      = Kp * error
 *   I     += Ki * error * dt
 *   u      = P + I    → returned to caller → SetMaxPollRecords
 */
public final class ControllerPI {
    private final double kp;
    private final double ki;
    private final double targetPollRecords;
    private final double dt;

    private double integral;

    /** @param dt Sampling period in seconds (d in Algorithm 1). */
    public ControllerPI(double kp, double ki, double targetPollRecords, double dt) {
        this.kp                = kp;
        this.ki                = ki;
        this.targetPollRecords = targetPollRecords;
        this.dt                = dt;
    }

    /** Update(y) from Algorithm 1 — returns control output u. */
    public double compute(double measuredRate) {
        double error = targetPollRecords - measuredRate;

        integral += ki * error * dt;

        double u = kp * error + integral;

        System.out.printf("[PI] y=%.2f  target=%.0f  error=%.2f  P=%.2f  I=%.2f  u=%.2f%n",
                measuredRate, targetPollRecords, error, kp * error, integral, u);

        return u;
    }
}
