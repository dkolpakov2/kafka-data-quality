package com.us.dq;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class ZScoreProcessFunction extends RichMapFunction<JsonNode, JsonNode> {

    private transient ValueState<StatsState> statsState;
    private final double threshold;

    public ZScoreProcessFunction(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<StatsState> descriptor =
                new ValueStateDescriptor<>("statsState", StatsState.class);
        statsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public JsonNode map(JsonNode node) throws Exception {
        double value = node.get("value").asDouble();

        StatsState state = statsState.value();
        if (state == null) {
            state = new StatsState();
        }

        // Compute stats BEFORE updating for anomaly detection
        double mean = state.mean;
        double stddev = state.getStdDev();
        double z = (stddev == 0.0) ? 0.0 : (value - mean) / stddev;
        boolean anomaly = Math.abs(z) > threshold;

        // Attach anomaly flag to JSON
        ((com.fasterxml.jackson.databind.node.ObjectNode) node)
                .put("zscore", z)
                .put("isAnomaly", anomaly);

        // Update state AFTER evaluating anomaly
        state.update(value);
        statsState.update(state);

        return node;
    }
}
