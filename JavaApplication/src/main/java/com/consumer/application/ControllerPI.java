package com.consumer.application;

/**
 * Pure PI controller. Receives measured rate y (rec/s) from Monitor.
 * The sampling period dt is fixed at construction time (Algorithm 1).
 *
 *   error  = targetRate - y
 *   P      = Kp * error
 *   I     += Ki * error * dt   (frozen when u is saturated — anti-windup)
 *   u      = P + I    → returned to caller → SetMaxPollRecords
 */
public final class ControllerPI {
    private final double kp;
    private final double ki;
    private       double targetRate;  // desired throughput in msg/s — mutable for step reference
    private final double dt;
    private final double uMin;
    private final double uMax;

    private double integral;

    /** @param dt Sampling period in seconds. @param uMin/@param uMax output limits (= maxPollRecords bounds). */
    public ControllerPI(double kp, double ki, double targetRate, double dt, double uMin, double uMax) {
        this.kp         = kp;
        this.ki         = ki;
        this.targetRate = targetRate;
        this.dt         = dt;
        this.uMin       = uMin;
        this.uMax       = uMax;
    }

    public void setTargetRate(double targetRate) {
        this.targetRate = targetRate;
    }

    public double getTargetRate() {
        return targetRate;
    }

    /** Update(y) from Algorithm 1 — returns control output u (clamped to [uMin, uMax]). */
    public double compute(double measuredRate) {
        double error = targetRate - measuredRate;

        double u_unsat = kp * error + integral;
        double u       = Math.max(uMin, Math.min(uMax, u_unsat));

        // anti-windup: only accumulate integral when output is not saturated
        if (u == u_unsat) {
            integral += ki * error * dt;
        }

        System.out.printf("[PI] y=%.2f  target=%.0f  error=%.2f  P=%.2f  I=%.2f  u=%.2f%n",
                measuredRate, targetRate, error, kp * error, integral, u);

        return u;
    }
}
