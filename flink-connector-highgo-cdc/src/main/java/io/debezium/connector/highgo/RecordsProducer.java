/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.highgo;

import io.debezium.function.BlockingConsumer;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Class which generates Kafka Connect {@link org.apache.kafka.connect.source.SourceRecord} records.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public abstract class RecordsProducer {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final io.debezium.connector.highgo.PostgresTaskContext taskContext;
    protected final SourceInfo sourceInfo;

    protected RecordsProducer(PostgresTaskContext taskContext, SourceInfo sourceInfo) {
        assert taskContext != null;
        assert sourceInfo != null;

        this.sourceInfo = sourceInfo;
        this.taskContext = taskContext;
    }

    /**
     * Starts up this producer. This is normally done by a {@link
     * io.debezium.connector.highgo.PostgresConnectorTask} instance. Subclasses should start
     * enqueuing records via a separate thread at the end of this method.
     *
     * @param recordsConsumer a consumer of {@link io.debezium.connector.highgo.ChangeEvent}
     *     instances, may not be null
     */
    protected abstract void start(
            BlockingConsumer<ChangeEvent> recordsConsumer, Consumer<Throwable> failureConsumer);

    /** Notification that offsets have been committed to Kafka up to the given LSN. */
    protected abstract void commit(long lsn);

    /**
     * Requests that this producer be stopped. This is normally a request coming from a {@link
     * io.debezium.connector.highgo.PostgresConnectorTask} instance
     */
    protected abstract void stop();

    protected PostgresSchema schema() {
        return taskContext.schema();
    }

    protected TopicSelector<TableId> topicSelector() {
        return taskContext.topicSelector();
    }

    protected Clock clock() {
        return taskContext.getClock();
    }
}
