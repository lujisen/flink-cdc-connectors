/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.highgo;

import io.debezium.connector.highgo.connection.PostgresConnection;
import io.debezium.connector.highgo.connection.ReplicationConnection;
import io.debezium.connector.highgo.spi.SlotCreationResult;
import io.debezium.connector.highgo.spi.SlotState;
import io.debezium.connector.highgo.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.*;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import java.util.Optional;

public class PostgresChangeEventSourceFactory
        implements ChangeEventSourceFactory<
                io.debezium.connector.highgo.PostgresPartition,
                io.debezium.connector.highgo.PostgresOffsetContext> {

    private final io.debezium.connector.highgo.PostgresConnectorConfig configuration;
    private final PostgresConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final io.debezium.connector.highgo.PostgresEventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final io.debezium.connector.highgo.PostgresSchema schema;
    private final io.debezium.connector.highgo.PostgresTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;

    public PostgresChangeEventSourceFactory(
            PostgresConnectorConfig configuration,
            Snapshotter snapshotter,
            PostgresConnection jdbcConnection,
            ErrorHandler errorHandler,
            PostgresEventDispatcher<TableId> dispatcher,
            Clock clock,
            PostgresSchema schema,
            PostgresTaskContext taskContext,
            ReplicationConnection replicationConnection,
            SlotCreationResult slotCreatedInfo,
            SlotState startingSlotInfo) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
    }

    @Override
    public SnapshotChangeEventSource<
                    io.debezium.connector.highgo.PostgresPartition,
                    io.debezium.connector.highgo.PostgresOffsetContext>
            getSnapshotChangeEventSource(
                    SnapshotProgressListener<io.debezium.connector.highgo.PostgresPartition>
                            snapshotProgressListener) {
        return new PostgresSnapshotChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                slotCreatedInfo,
                startingSlotInfo);
    }

    @Override
    public StreamingChangeEventSource<
                    io.debezium.connector.highgo.PostgresPartition,
                    io.debezium.connector.highgo.PostgresOffsetContext>
            getStreamingChangeEventSource() {
        return new PostgresStreamingChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                replicationConnection);
    }

    @Override
    public Optional<
                    IncrementalSnapshotChangeEventSource<
                            io.debezium.connector.highgo.PostgresPartition,
                            ? extends DataCollectionId>>
            getIncrementalSnapshotChangeEventSource(
                    PostgresOffsetContext offsetContext,
                    SnapshotProgressListener<io.debezium.connector.highgo.PostgresPartition>
                            snapshotProgressListener,
                    DataChangeEventListener<PostgresPartition> dataChangeEventListener) {
        // If no data collection id is provided, don't return an instance as the implementation
        // requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }
        final io.debezium.connector.highgo.PostgresSignalBasedIncrementalSnapshotChangeEventSource
                incrementalSnapshotChangeEventSource =
                        new PostgresSignalBasedIncrementalSnapshotChangeEventSource(
                                configuration,
                                jdbcConnection,
                                dispatcher,
                                schema,
                                clock,
                                snapshotProgressListener,
                                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
