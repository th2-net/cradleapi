/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.intervals;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.intervals.TimeInterval;
import com.exactpro.cradle.intervals.RecoveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.CRAWLER_TYPE;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALED_EVENT_IDS;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_END_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_LAST_UPDATE_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_LAST_UPDATE_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.INTERVAL_START_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.RECOVERY_STATE_JSON;

@Entity
public class TimeIntervalEntity
{
    private static final Logger logger = LoggerFactory.getLogger(TimeIntervalEntity.class);

    @PartitionKey(0)
    @CqlName(INSTANCE_ID)
    private UUID instanceId;

    @PartitionKey(1)
    @CqlName(INTERVAL_DATE)
    private LocalDate date;

    @ClusteringColumn(0)
    @CqlName(INTERVAL_START_TIME)
    private LocalTime startTime;

    @PartitionKey(2)
    @CqlName(CRAWLER_TYPE)
    private String crawlerType;

    @CqlName(INTERVAL_END_TIME)
    private LocalTime endTime;

    @CqlName(INTERVAL_LAST_UPDATE_DATE)
    private LocalDate lastUpdateDate;

    @CqlName(INTERVAL_LAST_UPDATE_TIME)
    private LocalTime lastUpdateTime;

    @CqlName(RECOVERY_STATE_JSON)
    private String recoveryStateJson;

    @CqlName(HEALED_EVENT_IDS)
    private Set<String> healedEventsIds;

    public TimeIntervalEntity()
    {
    }

    public TimeIntervalEntity(TimeInterval timeInterval, UUID instanceId)
    {
        this.startTime = timeInterval.getStartTime();
        this.endTime = timeInterval.getEndTime();
        this.date = timeInterval.getDate();
        this.lastUpdateTime = timeInterval.getLastUpdateTime();
        this.lastUpdateDate = timeInterval.getLastUpdateDate();
        this.crawlerType = timeInterval.getCrawlerType();
        this.recoveryStateJson = timeInterval.getRecoveryState().convertToJson();
        this.instanceId = instanceId;
        this.healedEventsIds = new HashSet<>();
    }

    public UUID getInstanceId()
    {
        return instanceId;
    }

    public void setInstanceId(UUID instanceId)
    {
        this.instanceId = instanceId;
    }

    public LocalTime getStartTime() { return startTime; }

    public void setStartTime(LocalTime startTime) { this.startTime = startTime; }

    public LocalTime getEndTime() { return endTime; }

    public void setEndTime(LocalTime endTime) { this.endTime = endTime; }

    public LocalDate getDate() { return date; }

    public void setDate(LocalDate date) { this.date = date; }

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public void setLastUpdateDate(LocalDate lastUpdateDate) { this.lastUpdateDate = lastUpdateDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public void setLastUpdateTime(LocalTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }

    public String getRecoveryStateJson() { return recoveryStateJson; }

    public void setRecoveryStateJson(String recoveryStateJson) { this.recoveryStateJson = recoveryStateJson; }

    public String getCrawlerType() { return crawlerType; }

    public void setCrawlerType(String crawlerType) { this.crawlerType = crawlerType; }

    public Set<String> getHealedEventsIds() { return healedEventsIds; }

    public void setHealedEventsIds(Set<String> healedEventsIds) { this.healedEventsIds = healedEventsIds; }

    public TimeInterval asTimeInterval() throws IOException {
        return new TimeInterval(this.startTime, this.endTime, this.date,
                RecoveryState.getMAPPER().readValue(recoveryStateJson, RecoveryState.class), this.getLastUpdateDate(), this.getLastUpdateTime(), this.crawlerType);
    }
}
