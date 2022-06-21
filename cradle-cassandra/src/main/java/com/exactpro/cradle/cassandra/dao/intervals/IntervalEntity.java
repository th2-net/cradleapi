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

package com.exactpro.cradle.cassandra.dao.intervals;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.RecoveryState;

import java.io.IOException;
import java.time.*;


@Entity
@CqlName(IntervalEntity.TABLE_NAME)
public class IntervalEntity {
    public static final String TABLE_NAME = "intervals";
    public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

    public static final String FIELD_BOOK = "book";
    public static final String FIELD_PAGE = "page";
    public static final String FIELD_INTERVAL_START_DATE = "interval_start_date";
    public static final String FIELD_CRAWLER_NAME = "crawler_name";
    public static final String FIELD_CRAWLER_VERSION = "crawler_version";
    public static final String FIELD_CRAWLER_TYPE = "crawler_type";
    public static final String FIELD_INTERVAL_START_TIME = "interval_start_time";
    public static final String FIELD_INTERVAL_END_DATE = "interval_end_date";
    public static final String FIELD_INTERVAL_END_TIME = "interval_end_time";
    public static final String FIELD_INTERVAL_LAST_UPDATE_DATE = "interval_last_update_date";
    public static final String FIELD_INTERVAL_LAST_UPDATE_TIME = "interval_last_update_time";
    public static final String FIELD_RECOVERY_STATE_JSON = "recovery_state_json";
    public static final String FIELD_INTERVAL_PROCESSED = "interval_processed";

    @PartitionKey(1)
    @CqlName(FIELD_BOOK)
    private String book;

    @PartitionKey(2)
    @CqlName(FIELD_PAGE)
    private String page;

    @PartitionKey(3)
    @CqlName(FIELD_INTERVAL_START_DATE)
    private LocalDate startDate;

    @ClusteringColumn(1)
    @CqlName(FIELD_CRAWLER_NAME)
    private String crawlerName;

    @ClusteringColumn(2)
    @CqlName(FIELD_CRAWLER_VERSION)
    private String crawlerVersion;

    @ClusteringColumn(3)
    @CqlName(FIELD_CRAWLER_TYPE)
    private String crawlerType;

    @ClusteringColumn(4)
    @CqlName(FIELD_INTERVAL_START_TIME)
    private LocalTime startTime;

    @CqlName(FIELD_INTERVAL_END_DATE)
    private LocalDate endDate;

    @CqlName(FIELD_INTERVAL_END_TIME)
    private LocalTime endTime;

    @CqlName(FIELD_INTERVAL_LAST_UPDATE_DATE)
    private LocalDate lastUpdateDate;

    @CqlName(FIELD_INTERVAL_LAST_UPDATE_TIME)
    private LocalTime lastUpdateTime;

    @CqlName(FIELD_RECOVERY_STATE_JSON)
    private String recoveryStateJson;

    @CqlName(FIELD_INTERVAL_PROCESSED)
    private boolean processed;

    public IntervalEntity() {
    }

    public IntervalEntity(Interval interval, PageId pageId) {
        this.startTime = LocalTime.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET));
        this.endTime = LocalTime.from(interval.getEndTime().atOffset(TIMEZONE_OFFSET));
        this.startDate = LocalDate.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET));
        this.endDate = LocalDate.from(interval.getEndTime().atOffset(TIMEZONE_OFFSET));
        this.lastUpdateTime = LocalTime.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET));
        this.lastUpdateDate = LocalDate.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET));
        this.recoveryStateJson = interval.getRecoveryState().convertToJson();
        this.page = pageId.getName();
        this.book = pageId.getBookId().getName();
        this.crawlerName = interval.getCrawlerName();
        this.crawlerVersion = interval.getCrawlerVersion();
        this.crawlerType = interval.getCrawlerType();
        this.processed = interval.isProcessed();
    }

    public String getBook() {
        return book;
    }

    public void setBook(String book) {
        this.book = book;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    public LocalTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalTime startTime) {
        this.startTime = startTime;
    }

    public LocalTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalTime endTime) {
        this.endTime = endTime;
    }

    public LocalDate getLastUpdateDate() {
        return lastUpdateDate;
    }

    public void setLastUpdateDate(LocalDate lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }

    public LocalTime getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(LocalTime lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getRecoveryStateJson() {
        return recoveryStateJson;
    }

    public void setRecoveryStateJson(String recoveryStateJson) {
        this.recoveryStateJson = recoveryStateJson;
    }

    public String getCrawlerName() {
        return crawlerName;
    }

    public void setCrawlerName(String crawlerName) {
        this.crawlerName = crawlerName;
    }

    public String getCrawlerVersion() {
        return crawlerVersion;
    }

    public void setCrawlerVersion(String crawlerVersion) {
        this.crawlerVersion = crawlerVersion;
    }

    public String getCrawlerType() {
        return crawlerType;
    }

    public void setCrawlerType(String crawlerType) {
        this.crawlerType = crawlerType;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public Interval asInterval() throws IOException {
        return Interval.builder().startTime(Instant.from(LocalDateTime.of(startDate, startTime).atOffset(TIMEZONE_OFFSET)))
                .endTime(Instant.from(LocalDateTime.of(endDate, endTime).atOffset(TIMEZONE_OFFSET)))
                .recoveryState(RecoveryState.getMAPPER().readValue(recoveryStateJson, RecoveryState.class))
                .lastUpdateTime(Instant.from(LocalDateTime.of(lastUpdateDate, lastUpdateTime).atOffset(TIMEZONE_OFFSET)))
                .crawlerName(crawlerName).crawlerVersion(crawlerVersion).crawlerType(crawlerType)
                .processed(processed).build();
    }
}
