/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.highgo.connection;

import io.debezium.connector.highgo.TypeRegistry;
import io.debezium.connector.highgo.connection.ReplicationStream.ReplicationMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Abstract implementation of {@link io.debezium.connector.highgo.connection.MessageDecoder} that
 * all decoders should inherit from.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageDecoder.class);

    @Override
    public void processMessage(
            ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {
        // if message is empty pass control right to ReplicationMessageProcessor to update WAL
        // position info
        if (buffer == null) {
            processor.process(null);
        } else {
            processNotEmptyMessage(buffer, processor, typeRegistry);
        }
    }

    protected abstract void processNotEmptyMessage(
            ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException;

    @Override
    public boolean shouldMessageBeSkipped(
            ByteBuffer buffer,
            io.debezium.connector.highgo.connection.Lsn lastReceivedLsn,
            Lsn startLsn,
            WalPositionLocator walPosition) {
        // the lsn we started from is inclusive, so we need to avoid sending back the same message
        // twice
        // but for the first record seen ever it is possible we received the same LSN as the one
        // obtained from replication slot
        if (walPosition.skipMessage(lastReceivedLsn)) {
            LOGGER.info(
                    "Streaming requested from LSN {}, received LSN {} identified as already processed",
                    startLsn,
                    lastReceivedLsn);
            return true;
        }
        return false;
    }

    @Override
    public void close() {}
}
