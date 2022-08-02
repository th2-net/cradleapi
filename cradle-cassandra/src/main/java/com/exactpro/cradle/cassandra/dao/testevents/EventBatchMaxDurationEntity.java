/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.time.LocalDate;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class EventBatchMaxDurationEntity {

    @PartitionKey(0)
    @CqlName(INSTANCE_ID)
    private UUID instanceId;

    @PartitionKey(1)
    @CqlName(START_DATE)
    private LocalDate startDate;

    @CqlName(MAX_BATCH_DURATION)
    private long maxBatchDuration;

    public EventBatchMaxDurationEntity() {

    }

    public EventBatchMaxDurationEntity(UUID instanceId, LocalDate startDate, long maxBatchDuration) {
        this.instanceId = instanceId;
        this.startDate = startDate;
        this.maxBatchDuration = maxBatchDuration;
    }

    public UUID getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(UUID instanceId) {
        this.instanceId = instanceId;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    public long getMaxBatchDuration() {
        return maxBatchDuration;
    }

    public void setMaxBatchDuration(long maxBatchDuration) {
        this.maxBatchDuration = maxBatchDuration;
    }
}
