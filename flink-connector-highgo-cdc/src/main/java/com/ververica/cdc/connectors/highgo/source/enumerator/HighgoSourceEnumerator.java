/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.highgo.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.highgo.source.HighgoDialect;
import com.ververica.cdc.connectors.highgo.source.config.HighgoSourceConfig;
import io.debezium.connector.highgo.connection.PostgresConnection;
import io.debezium.connector.highgo.connection.PostgresReplicationConnection;
import io.debezium.connector.highgo.spi.SlotState;

import java.sql.SQLException;

/**
 * The Postgres source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
public class HighgoSourceEnumerator extends IncrementalSourceEnumerator {

    private final HighgoDialect postgresDialect;

    public HighgoSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            HighgoSourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            HighgoDialect postgresDialect) {
        super(context, sourceConfig, splitAssigner);
        this.postgresDialect = postgresDialect;
    }

    @Override
    public void start() {
        createSlotForGlobalStreamSplit();
        super.start();
    }

    /**
     * Create slot for the unique global stream split.
     *
     * <p>Currently all startup modes need read the stream split. We need open the slot before
     * reading the globalStreamSplit to catch all data changes.
     */
    private void createSlotForGlobalStreamSplit() {
        SlotState slotInfo = null;
        try (PostgresConnection connection = postgresDialect.openJdbcConnection()) {
            System.out.println("connection:::::::::::"+ connection.toString());
            System.out.println("connection:::::::::::"+ connection.config().getConnectionFactoryClassName());
            System.out.println("connection::getSlotName:::::::::"+  postgresDialect.getSlotName());
            System.out.println("connection::::getPluginName:::::::"+  postgresDialect.getPluginName());
            slotInfo =
                    connection.getReplicationSlotState(
                            postgresDialect.getSlotName(), postgresDialect.getPluginName());
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format(
                            "Fail to get the replication slot info, the slot name is %s.",
                            postgresDialect.getSlotName()),
                    e);
        }

        // skip creating the replication slot when the slot exists.
        if (slotInfo != null) {
            return;
        }

        try {
            PostgresReplicationConnection replicationConnection =
                    postgresDialect.openPostgresReplicationConnection();
            replicationConnection.createReplicationSlot();
            replicationConnection.close(false);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    "Create Slot For Global Stream Split failed due to ", t);
        }
    }
}
