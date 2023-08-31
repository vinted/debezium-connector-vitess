package io.debezium.connector.vitess.metrics;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.util.Collect;

public class VgtidResetMetric extends CustomMetrics implements VgtidResetMetricMBean {
    private final AtomicLong numberOfVgtidResets = new AtomicLong();

    public VgtidResetMetric(String connectorType, String taskId, VitessConnectorConfig config) {
        super(connectorType,
                Collect.linkMapOf("taskId", String.format("%s-%s", taskId, UUID.randomUUID().toString()), "shard", config.getKeyspace(), "tables",
                        config.tableIncludeList() == null ? "no_table" : config.tableIncludeList()));
    }

    @Override
    public long getNumberOfVgtidResets() {
        return numberOfVgtidResets.get();
    }

    @Override
    public void resetVgtid() {
        numberOfVgtidResets.incrementAndGet();
    }

    @Override
    public void reset() {
        numberOfVgtidResets.set(0);
    }
}
