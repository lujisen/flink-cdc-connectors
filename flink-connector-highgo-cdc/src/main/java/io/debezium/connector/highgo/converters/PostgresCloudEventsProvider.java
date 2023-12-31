/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.highgo.converters;

import io.debezium.connector.highgo.Module;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * An implementation of {@link CloudEventsProvider} for PostgreSQL.
 *
 * @author Chris Cranford
 */
public class PostgresCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public RecordParser createParser(Schema schema, Struct record) {
        return new PostgresRecordParser(schema, record);
    }

    @Override
    public CloudEventsMaker createMaker(
            RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new PostgresCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}
