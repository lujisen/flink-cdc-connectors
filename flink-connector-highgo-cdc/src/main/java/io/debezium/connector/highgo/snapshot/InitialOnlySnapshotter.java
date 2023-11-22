/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.highgo.snapshot;

import io.debezium.connector.highgo.PostgresConnectorConfig;
import io.debezium.connector.highgo.spi.OffsetState;
import io.debezium.connector.highgo.spi.SlotState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitialOnlySnapshotter extends QueryingSnapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitialOnlySnapshotter.class);
    private OffsetState sourceInfo;

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        super.init(config, sourceInfo, slotState);
        this.sourceInfo = sourceInfo;
    }

    @Override
    public boolean shouldStream() {
        return false;
    }

    @Override
    public boolean shouldSnapshot() {
        if (sourceInfo == null) {
            LOGGER.info("Taking initial snapshot for new datasource");
            return true;
        } else if (sourceInfo.snapshotInEffect()) {
            LOGGER.info("Found previous incomplete snapshot");
            return true;
        } else {
            LOGGER.info("Previous initial snapshot completed, no snapshot will be performed");
            return false;
        }
    }
}
