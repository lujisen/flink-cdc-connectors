/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.highgo.snapshot;

import io.debezium.connector.highgo.PostgresConnectorConfig;
import io.debezium.connector.highgo.spi.OffsetState;
import io.debezium.connector.highgo.spi.SlotState;
import io.debezium.connector.highgo.spi.Snapshotter;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class NeverSnapshotter implements Snapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NeverSnapshotter.class);

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        if (sourceInfo != null && sourceInfo.snapshotInEffect()) {
            String msg =
                    "The connector previously stopped while taking a snapshot, but now the connector is configured "
                            + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.";
            LOGGER.error(msg);
            throw new ConnectException(msg);
        } else {
            LOGGER.info(
                    "Snapshots are not allowed as per configuration, starting streaming logical changes only");
        }
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshot() {
        return false;
    }

    @Override
    public Optional<String> buildSnapshotQuery(
            TableId tableId, List<String> snapshotSelectColumns) {
        throw new UnsupportedOperationException("'never' snapshot mode cannot build queries");
    }
}
