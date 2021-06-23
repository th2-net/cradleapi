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
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.RecoveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class IntervalEntity {
    private static final Logger logger = LoggerFactory.getLogger(IntervalEntity.class);

    @PartitionKey(0)
    @CqlName(INSTANCE_ID)
    private UUID instanceId;

    @PartitionKey(1)
    @CqlName(INTERVAL_START_DATE)
    private LocalDate startDate;

    @ClusteringColumn(0)
    @CqlName(CRAWLER_NAME)
    private String crawlerName;

    @ClusteringColumn(1)
    @CqlName(CRAWLER_VERSION)
    private String crawlerVersion;

    @ClusteringColumn(2)
    @CqlName(INTERVAL_START_TIME)
    private LocalTime startTime;

    @CqlName(INTERVAL_END_DATE)
    private LocalDate endDate;

    @CqlName(INTERVAL_END_TIME)
    private LocalTime endTime;

    @CqlName(INTERVAL_LAST_UPDATE_DATE)
    private LocalDate lastUpdateDate;

    @CqlName(INTERVAL_LAST_UPDATE_TIME)
    private LocalTime lastUpdateTime;

    @CqlName(INTERVAL_ID)
    private String id;

    @CqlName(RECOVERY_STATE_JSON)
    private String recoveryStateJson;

    @CqlName(INTERVAL_PROCESSED)
    private boolean processed;

    public IntervalEntity()
    {
    }

    public IntervalEntity(Interval interval, UUID instanceId)
    {
        this.id = interval.getId();
        this.startTime = interval.getStartDateTime().toLocalTime();
        this.endTime = interval.getEndDateTime().toLocalTime();
        this.startDate = interval.getStartDateTime().toLocalDate();
        this.endDate = interval.getEndDateTime().toLocalDate();
        this.lastUpdateTime = interval.getLastUpdateDateTime().toLocalTime();
        this.lastUpdateDate = interval.getLastUpdateDateTime().toLocalDate();
        this.recoveryStateJson = interval.getRecoveryState().convertToJson();
        this.instanceId = instanceId;
        this.crawlerName = interval.getCrawlerName();
        this.crawlerVersion = interval.getCrawlerVersion();
        this.processed = interval.isProcessed();
    }

    public UUID getInstanceId()
    {
        return instanceId;
    }

    public void setInstanceId(UUID instanceId)
    {
        this.instanceId = instanceId;
    }

    public String getId() { return id; }

    public void setId(String id) { this.id = id; }

    public LocalDate getStartDate() { return startDate; }

    public void setStartDate(LocalDate startDate) { this.startDate = startDate; }

    public LocalTime getStartTime() { return startTime; }

    public void setStartTime(LocalTime startTime) { this.startTime = startTime; }

    public LocalTime getEndTime() { return endTime; }

    public void setEndTime(LocalTime endTime) { this.endTime = endTime; }

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public void setLastUpdateDate(LocalDate lastUpdateDate) { this.lastUpdateDate = lastUpdateDate; }

    public LocalDate getEndDate() { return endDate; }

    public void setEndDate(LocalDate endDate) { this.endDate = endDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public void setLastUpdateTime(LocalTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }

    public String getRecoveryStateJson() { return recoveryStateJson; }

    public void setRecoveryStateJson(String recoveryStateJson) { this.recoveryStateJson = recoveryStateJson; }

    public String getCrawlerName() { return crawlerName; }

    public void setCrawlerName(String crawlerName) { this.crawlerName = crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    public void setCrawlerVersion(String crawlerVersion) { this.crawlerVersion = crawlerVersion; }

    public boolean isProcessed() { return processed; }

    public void setProcessed(boolean processed) { this.processed = processed; }

    public Interval asInterval() throws IOException {
        return Interval.builder().id(id).startDateTime(LocalDateTime.of(startDate, startTime))
                .endDateTime(LocalDateTime.of(endDate, endTime))
                .recoveryState(RecoveryState.getMAPPER().readValue(recoveryStateJson, RecoveryState.class))
                .lastUpdateDateTime(LocalDateTime.of(lastUpdateDate, lastUpdateTime))
                .crawlerName(crawlerName).crawlerVersion(crawlerVersion)
                .processed(processed).build();
    }
}
