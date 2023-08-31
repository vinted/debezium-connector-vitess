package io.debezium.connector.vitess.metrics;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;

/**
 * Base for metrics implementations.
 *
 * @author Jiri Pechanec
 */
@ThreadSafe
public abstract class CustomMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomMetrics.class);

    private final ObjectName name;
    private volatile boolean registered = false;

    protected CustomMetrics(String connectorType, Map<String, String> tags) {
        this.name = metricName(connectorType, tags);
    }

    /**
     * Registers a metrics MBean into the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void register() {
        try {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                LOGGER.info("JMX not supported, bean '{}' not registered", name);
                return;
            }
            LOGGER.info("JMX Registering metric {}", name);
            mBeanServer.registerMBean(this, name);
            registered = true;
        }
        catch (JMException e) {
            throw new RuntimeException("Unable to register the MBean '" + name + "'", e);
        }
    }

    /**
     * Unregisters a metrics MBean from the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void unregister() {
        if (this.name != null && registered) {
            try {
                final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                if (mBeanServer == null) {
                    LOGGER.debug("JMX not supported, bean '{}' not registered", name);
                    return;
                }
                mBeanServer.unregisterMBean(name);
                registered = false;
            }
            catch (JMException e) {
                throw new RuntimeException("Unable to unregister the MBean '" + name + "'", e);
            }
        }
    }

    /**
     * Create a JMX metric name for the given metric.
     * @return the JMX metric name
     */
    protected ObjectName metricName(String connectorType, Map<String, String> tags) {
        final String metricName = "debezium." + connectorType.toLowerCase() + ":type=connector-metrics,"
                + tags.entrySet().stream()
                        .map(e -> e.getKey() + "=" + Sanitizer.jmxSanitize(e.getValue()))
                        .collect(Collectors.joining(","));
        try {
            return new ObjectName(metricName);
        }
        catch (MalformedObjectNameException e) {
            throw new ConnectException("Invalid metric name '" + metricName + "'");
        }
    }
}
