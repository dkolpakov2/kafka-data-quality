package com.us.dq;

import java.io.Serializable;

public class StatsState implements Serializable {
    public long count = 0;
    public double mean = 0.0;
    public double m2 = 0.0; // Sum of squares of differences

    public void update(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        m2 += delta * delta2;
    }

    public double getStdDev() {
        return count > 1 ? Math.sqrt(m2 / (count - 1)) : 0.0;
    }
}

