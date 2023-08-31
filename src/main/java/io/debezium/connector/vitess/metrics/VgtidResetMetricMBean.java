package io.debezium.connector.vitess.metrics;

public interface VgtidResetMetricMBean {
    long getNumberOfVgtidResets();

    void resetVgtid();

    void reset();
}
