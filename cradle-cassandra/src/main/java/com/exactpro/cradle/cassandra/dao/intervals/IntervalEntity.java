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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class IntervalEntity {
    private static final Logger logger = LoggerFactory.getLogger(IntervalEntity.class);
    public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

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
    @CqlName(CRAWLER_TYPE)
    private String crawlerType;

    @ClusteringColumn(3)
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

    @CqlName(RECOVERY_STATE_JSON)
    private String recoveryStateJson;

    @CqlName(INTERVAL_PROCESSED)
    private boolean processed;

    public IntervalEntity()
    {
    }

    public IntervalEntity(Interval interval, UUID instanceId)
    {
        this.startTime = LocalTime.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET));
        this.endTime = LocalTime.from(interval.getEndTime().atOffset(TIMEZONE_OFFSET));
        this.startDate = LocalDate.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET));
        this.endDate = LocalDate.from(interval.getEndTime().atOffset(TIMEZONE_OFFSET));
        this.lastUpdateTime = LocalTime.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET));
        this.lastUpdateDate = LocalDate.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET));
        this.recoveryStateJson = interval.getRecoveryState();
        this.instanceId = instanceId;
        this.crawlerName = interval.getCrawlerName();
        this.crawlerVersion = interval.getCrawlerVersion();
        this.crawlerType = interval.getCrawlerType();
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

    public String getCrawlerType() { return crawlerType; }

    public void setCrawlerType(String crawlerType) { this.crawlerType = crawlerType; }

    public boolean isProcessed() { return processed; }

    public void setProcessed(boolean processed) { this.processed = processed; }

    public Interval asInterval() throws IOException {
        return Interval.builder().startTime(Instant.from(LocalDateTime.of(startDate, startTime).atOffset(TIMEZONE_OFFSET)))
                .endTime(Instant.from(LocalDateTime.of(endDate, endTime).atOffset(TIMEZONE_OFFSET)))
                .recoveryState(recoveryStateJson)
                .lastUpdateTime(Instant.from(LocalDateTime.of(lastUpdateDate, lastUpdateTime).atOffset(TIMEZONE_OFFSET)))
                .crawlerName(crawlerName).crawlerVersion(crawlerVersion).crawlerType(crawlerType)
                .processed(processed).build();
    }
}
