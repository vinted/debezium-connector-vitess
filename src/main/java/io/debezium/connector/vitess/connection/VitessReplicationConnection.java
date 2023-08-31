/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.connector.vitess.metrics.VgtidResetMetric;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.vitess.client.Proto;
import io.vitess.client.grpc.StaticAuthCredentials;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import io.vitess.proto.grpc.VitessGrpc;

import binlogdata.Binlogdata;
import binlogdata.Binlogdata.VEvent;

/**
 * Connection to VTGate to replication messages. Also connect to VTCtld to get the latest {@link
 * Vgtid} if no previous offset exists.
 */
public class VitessReplicationConnection implements ReplicationConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessReplicationConnection.class);

    private final MessageDecoder messageDecoder;
    private final VitessConnectorConfig config;
    // Channel closing is invoked from the change-event-source-coordinator thread
    private final AtomicReference<ManagedChannel> managedChannel = new AtomicReference<>();
    private final AtomicInteger internalRestarts = new AtomicInteger(5);

    private final VgtidResetMetric vgtidResetMetric;

    public VitessReplicationConnection(VitessConnectorConfig config, VitessDatabaseSchema schema, VgtidResetMetric vgtidResetMetric) {
        this.messageDecoder = new VStreamOutputMessageDecoder(schema);
        this.config = config;
        this.vgtidResetMetric = vgtidResetMetric;
    }

    /**
     * Execute SQL statement via vtgate gRPC.
     *
     * @param sqlStatement The SQL statement to be executed
     * @throws StatusRuntimeException if the connection is not valid, or SQL statement can not be successfully exected
     */
    public void execute(String sqlStatement) {
        ManagedChannel channel = newChannel(config.getVtgateHost(), config.getVtgatePort(), config.getGrpcMaxInboundMessageSize());
        managedChannel.compareAndSet(null, channel);

        Vtgate.ExecuteRequest request = Vtgate.ExecuteRequest.newBuilder()
                .setQuery(Proto.bindQuery(sqlStatement, Collections.emptyMap()))
                .build();
        newBlockingStub(channel).execute(request);
    }

    @Override
    public void startStreaming(
                               Vgtid vgtid, ReplicationMessageProcessor processor, AtomicReference<Throwable> error) {
        Objects.requireNonNull(vgtid);

        LOGGER.info("Vgtid eof handling enabled:" + config.getVgtidEofHandlingEnabled());
        ManagedChannel channel = newChannel(config.getVtgateHost(), config.getVtgatePort(), config.getGrpcMaxInboundMessageSize());
        managedChannel.compareAndSet(null, channel);

        VitessGrpc.VitessStub stub = newStub(channel);

        Map<String, String> grpcHeaders = config.getGrpcHeaders();
        if (!grpcHeaders.isEmpty()) {
            LOGGER.info("Setting VStream gRPC headers: {}", grpcHeaders);
            Metadata metadata = new Metadata();
            for (Map.Entry<String, String> entry : grpcHeaders.entrySet()) {
                metadata.put(Metadata.Key.of(entry.getKey(), Metadata.ASCII_STRING_MARSHALLER), entry.getValue());
            }
            stub = MetadataUtils.attachHeaders(stub, metadata);
        }

        StreamObserver<Vtgate.VStreamResponse> responseObserver = new StreamObserver<Vtgate.VStreamResponse>() {

            private Vgtid lastProcessedVgtid = null;

            @Override
            public void onNext(Vtgate.VStreamResponse response) {

                LOGGER.debug("Received {} VEvents in the VStreamResponse:",
                        response.getEventsCount());
                for (VEvent vEvent : response.getEventsList()) {
                    LOGGER.debug("VEvent: {}", vEvent);
                }

                Vgtid newVgtid = getVgtid(response);
                int numOfRowEvents = getNumOfRowEvents(response);

                try {
                    int rowEventSeen = 0;
                    for (int i = 0; i < response.getEventsCount(); i++) {
                        Binlogdata.VEvent vEvent = response.getEvents(i);
                        if (vEvent.getType() == Binlogdata.VEventType.ROW) {
                            rowEventSeen++;
                        }
                        boolean isLastRowEventOfTransaction = newVgtid != null && numOfRowEvents != 0 && rowEventSeen == numOfRowEvents;
                        messageDecoder.processMessage(response.getEvents(i), processor, newVgtid, isLastRowEventOfTransaction);
                    }
                    if (newVgtid != null) {
                        lastProcessedVgtid = newVgtid;
                    }
                }
                catch (InterruptedException e) {
                    LOGGER.error("Message processing is interrupted", e);
                    // Only propagate the first error
                    error.compareAndSet(null, e);
                    Thread.currentThread().interrupt();
                }
            }

            private Boolean isVitessEofException(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    Status status = ((StatusRuntimeException) t).getStatus();
                    return (status.getCode().equals(Status.UNKNOWN.getCode())
                            && status.getDescription() != null
                            && status.getDescription().contains("unexpected server EOF"));
                }
                else {
                    return false;
                }
            }

            private void restartStreaming(Vgtid startVgtid) {
                try {
                    close();
                }
                catch (Exception e) {
                    LOGGER.warn("Closing vitess connection error.", e);
                }
                startStreaming(startVgtid, processor, error);
            }

            @Override
            public void onError(Throwable t) {
                if (isVitessEofException(t)) {
                    // mitigate Vitess EOF exception when initial load is done
                    Vgtid currentVgtid = lastProcessedVgtid != null ? lastProcessedVgtid : vgtid;

                    LOGGER.warn(
                            String.format(
                                    "Initial vgid:%s, "
                                            + "lastProcessedVgtid:%s, "
                                            + "vgtidEofHandlingEnabled: %b, "
                                            + "internalRestarts: %s, "
                                            + "keyspace: %s, "
                                            + "tables: %s, ",
                                    vgtid,
                                    lastProcessedVgtid,
                                    config.getVgtidEofHandlingEnabled(),
                                    internalRestarts.get(),
                                    config.getKeyspace(),
                                    config.tableIncludeList()));

                    if (internalRestarts.get() > 0) {
                        String message = String.format(
                                "Vitess connection for keyspace:%s and tables: %s  was closed. Restart #:%s. Using Vgtid:%s",
                                config.getKeyspace(), config.tableIncludeList(), internalRestarts.decrementAndGet(), currentVgtid);
                        LOGGER.warn(message, t);
                        restartStreaming(currentVgtid);
                    }
                    // Mitigate vgtid expired with EOF exception in case SKIP is enabled
                    else if (internalRestarts.get() == 0
                            && Objects.equals(currentVgtid, vgtid)
                            && config.getVgtidEofHandlingEnabled()) {
                        Vgtid latestExistingVgtid = defaultVgtid(config);
                        String message = String.format(
                                "Vitess connection for keyspace:%s and tables: %s was closed and didn't recover. "
                                        + "Restart #:%s. Vgtid:%s is probably expired, skipping to latest Vgtid:%s ",
                                config.getKeyspace(), config.tableIncludeList(), vgtid, internalRestarts.getAndDecrement(), latestExistingVgtid);
                        LOGGER.warn(message, t);
                        restartStreaming(latestExistingVgtid);
                        vgtidResetMetric.resetVgtid();
                    }
                    else {
                        LOGGER.error(String.format(
                                "VStream streaming for keyspace:%s and tables: %s  onError. Status: %s",
                                config.getKeyspace(),
                                config.tableIncludeList(),
                                Status.fromThrowable(t)), t);
                        error.compareAndSet(null, t);
                    }

                }
                else {
                    LOGGER.error(String.format(
                            "VStream streaming for keyspace:%s and tables: %s  onError. Status: %s",
                            config.getKeyspace(),
                            config.tableIncludeList(),
                            Status.fromThrowable(t)), t);
                    error.compareAndSet(null, t);
                }
            }

            @Override
            public void onCompleted() {
                LOGGER.error("VStream streaming completed.");
            }

            // We assume there is at most one vgtid event for response.
            // Even in case of resharding, there is only one vgtid event that contains multiple shard
            // gtids.
            private Vgtid getVgtid(Vtgate.VStreamResponse response) {
                LinkedList<Vgtid> vgtids = new LinkedList<>();
                for (VEvent vEvent : response.getEventsList()) {
                    if (vEvent.getType() == Binlogdata.VEventType.VGTID) {
                        vgtids.addLast(Vgtid.of(vEvent.getVgtid()));
                    }
                }
                if (vgtids.size() == 0) {
                    // The VStreamResponse that contains an VERSION vEvent does not have VGTID.
                    // We do not update lastReceivedVgtid in this case.
                    // It can also be null if the 1st grpc response does not have vgtid upon restart
                    LOGGER.trace("No vgtid found in response {}...", response.toString().substring(0, Math.min(100, response.toString().length())));
                    LOGGER.debug("Full response is {}", response);
                    return null;
                }
                if (vgtids.size() > 1) {
                    LOGGER.error(
                            "Should only have 1 vgtid per VStreamResponse, but found {}. Use the last vgtid {}.",
                            vgtids.size(), vgtids.getLast());
                }
                return vgtids.getLast();
            }

            private int getNumOfRowEvents(Vtgate.VStreamResponse response) {
                int num = 0;
                for (VEvent vEvent : response.getEventsList()) {
                    if (vEvent.getType() == Binlogdata.VEventType.ROW) {
                        num++;
                    }
                }
                return num;
            }
        };

        Vtgate.VStreamFlags vStreamFlags = Vtgate.VStreamFlags.newBuilder()
                .setStopOnReshard(config.getStopOnReshard())
                .build();

        Topodata.TabletType tabletType = toTopodataTabletType(VitessTabletType.valueOf(config.getTabletType()));

        String[] includedTables = Optional.ofNullable(config.tableIncludeList()).orElse("").split(",");
        String[] excludedTables = Optional.ofNullable(config.tableExcludeList()).orElse("").split(",");

        List<String> explicitTables = Arrays.stream(includedTables)
                .filter(element -> !Arrays.asList(excludedTables).contains(element))
                .map(table -> table.replaceFirst(".*\\.", ""))
                .collect(Collectors.toList());

        // Providing a vgtid MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-61 here will make
        // VStream to start receiving row-changes from
        // MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-62

        // Adding table filter to address gho migration issues
        // https://github.com/vitessio/vitess/blob/fa2f2c066dc4175fea1955ba31f75ef0c7aed58d/proto/binlogdata.proto#L132

        // Table filter should address missing gho table schema decoration issue
        // Caused by: java.lang.RuntimeException: io.grpc.StatusRuntimeException: UNKNOWN: target:
        // xxxxx.0.replica: vttablet: rpc error:
        // code = Unknown desc = stream (at source tablet) error unknown table _xxxxxx_ghc in
        // schema at io.debezium.connector.vitess.VitessStreamingChangeEventSource.execute
        // (VitessStreamingChangeEventSource.java:75)

        if (explicitTables.size() > 0) {
            Binlogdata.Filter.Builder tableFilterBuilder = Binlogdata.Filter.newBuilder();
            for (String table : explicitTables) {
                String sql = "select * from " + table;
                // See rule in: https://github.com/vitessio/vitess/blob/release-14.0/go/vt/vttablet/tabletserver/vstreamer/planbuilder.go#L316
                Binlogdata.Rule rule = Binlogdata.Rule.newBuilder().setMatch(table).setFilter(sql).build();
                LOGGER.info("Add vstream table filtering: {}", rule.getMatch());
                tableFilterBuilder.addRules(rule);
            }
            Binlogdata.Filter tableFilter = tableFilterBuilder.build();
            stub.vStream(
                    Vtgate.VStreamRequest.newBuilder()
                            .setVgtid(vgtid.getRawVgtid())
                            .setTabletType(Objects.requireNonNull(tabletType))
                            .setFilter(tableFilter)
                            .setFlags(vStreamFlags)
                            .build(),
                    responseObserver);
        }
        else {
            stub.vStream(
                    Vtgate.VStreamRequest.newBuilder()
                            .setVgtid(vgtid.getRawVgtid())
                            .setTabletType(Objects.requireNonNull(tabletType))
                            .setFlags(vStreamFlags)
                            .build(),
                    responseObserver);
        }
    }

    private VitessGrpc.VitessStub newStub(ManagedChannel channel) {
        VitessGrpc.VitessStub stub = VitessGrpc.newStub(channel);
        return withCredentials(stub);
    }

    private VitessGrpc.VitessBlockingStub newBlockingStub(ManagedChannel channel) {
        VitessGrpc.VitessBlockingStub stub = VitessGrpc.newBlockingStub(channel);
        return withCredentials(stub);
    }

    private <T extends AbstractStub<T>> T withCredentials(T stub) {
        if (config.getVtgateUsername() != null && config.getVtgatePassword() != null) {
            LOGGER.info("Use authenticated vtgate grpc.");
            stub = stub.withCallCredentials(new StaticAuthCredentials(config.getVtgateUsername(), config.getVtgatePassword()));
        }
        return stub;
    }

    private ManagedChannel newChannel(String vtgateHost, int vtgatePort, int maxInboundMessageSize) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(vtgateHost, vtgatePort)
                .usePlaintext()
                .maxInboundMessageSize(maxInboundMessageSize)
                .keepAliveTime(config.getKeepaliveInterval().toMillis(), TimeUnit.MILLISECONDS)
                .build();
        return channel;
    }

    /**
     * Close the gRPC connection to VStream
     */
    @Override
    public void close() throws Exception {
        LOGGER.info("Closing replication connection");
        managedChannel.get().shutdownNow();
        LOGGER.trace("VStream GRPC channel shutdownNow is invoked.");
        if (managedChannel.get().awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.info("VStream GRPC channel is shutdown in time.");
        }
        else {
            LOGGER.warn("VStream GRPC channel is not shutdown in time. Give up waiting.");
        }
    }

    /**
     * Get latest replication position
     */
    public static Vgtid defaultVgtid(VitessConnectorConfig config) {
        if (config.getShard() == null || config.getShard().isEmpty()) {
            // Replicate all shards of the given keyspace
            final Vgtid gtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(config.getKeyspace())
                                            .setGtid(Vgtid.CURRENT_GTID)
                                            .build())
                            .build());
            LOGGER.info("Default VGTID '{}' is set to the current gtid of all shards from keyspace: {}", gtid, config.getKeyspace());
            return gtid;
        }
        else {
            String shardGtid = config.getGtid();
            String shard = config.getShard();
            String keyspace = config.getKeyspace();
            final Vgtid gtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(keyspace)
                                            .setShard(shard)
                                            .setGtid(shardGtid)
                                            .build())
                            .build());
            LOGGER.info("VGTID '{}' is set to the GTID {} for keyspace: {} shard: {}", gtid, shardGtid, config.getKeyspace(), shard);
            return gtid;
        }
    }

    public String connectionString() {
        return String.format("vtgate gRPC connection %s:%s", config.getVtgateHost(), config.getVtgatePort());
    }

    public String username() {
        return config.getVtgateUsername();
    }

    private static Topodata.TabletType toTopodataTabletType(VitessTabletType tabletType) {
        switch (tabletType) {
            case MASTER:
                return Topodata.TabletType.MASTER;
            case REPLICA:
                return Topodata.TabletType.REPLICA;
            case RDONLY:
                return Topodata.TabletType.RDONLY;
            default:
                LOGGER.warn("Unknown tabletType {}", tabletType);
                return null;
        }
    }
}
