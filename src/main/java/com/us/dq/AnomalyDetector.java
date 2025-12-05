package com.us.dq;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

// accumulator for mean/stddev
public class StatsAccumulator {
    public long count = 0;
    public double sum = 0.0;
    public double sumSq = 0.0;
}

// computes aggregates
public class StatsAggregate implements AggregateFunction<Double, StatsAccumulator, StatsAccumulator> {
    @Override
    public StatsAccumulator createAccumulator() {
        return new StatsAccumulator();
    }

    @Override
    public StatsAccumulator add(Double value, StatsAccumulator acc) {
        acc.count += 1;
        acc.sum += value;
        acc.sumSq += value * value;
        return acc;
    }

    @Override
    public StatsAccumulator getResult(StatsAccumulator acc) {
        return acc;
    }

    @Override
    public StatsAccumulator merge(StatsAccumulator a, StatsAccumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        a.sumSq += b.sumSq;
        return a;
    }
}

// after window aggregates are available, process to detect anomalies
public class AnomalyProcess extends ProcessWindowFunction<StatsAccumulator, String, String, TimeWindow> {
    private double zThreshold = 3.0;

    @Override
    public void process(String key, Context ctx, Iterable<StatsAccumulator> aggregates, Collector<String> out) throws Exception {
        StatsAccumulator acc = aggregates.iterator().next();
        double mean = acc.sum / acc.count;
        double variance = (acc.sumSq / acc.count) - (mean * mean);
        double std = Math.sqrt(Math.max(variance, 1e-9));
        // emit stats as JSON for downstream use; actual per-event detection is implemented elsewhere
        String statsJson = String.format("{\"key\":\"%s\",\"mean\":%f,\"std\":%f,\"count\":%d}", key, mean, std, acc.count);
        out.collect(statsJson);
    }
}
