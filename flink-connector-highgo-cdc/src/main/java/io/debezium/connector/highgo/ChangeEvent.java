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

package io.debezium.connector.highgo;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * A factory for creating various Debezium objects
 *
 * <p>It is a hack to access package-private constructor in debezium.
 */
public class ChangeEvent {
    private final SourceRecord record;
    private final Long lastCompletelyProcessedLsn;

    public ChangeEvent(SourceRecord record, Long lastCompletelyProcessedLsn) {
        this.record = record;
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
    }

    public SourceRecord getRecord() {
        return record;
    }

    public Long getLastCompletelyProcessedLsn() {
        return lastCompletelyProcessedLsn;
    }

    @Override
    public String toString() {
        return "ChangeEvent [record="
                + record
                + ", lastCompletelyProcessedLsn="
                + lastCompletelyProcessedLsn
                + "]";
    }
}
