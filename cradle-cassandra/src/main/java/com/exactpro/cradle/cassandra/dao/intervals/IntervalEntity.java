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

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.intervals.Interval;
import jnr.ffi.annotations.In;

import java.io.IOException;
import java.time.*;
import java.util.Objects;


@Entity
@CqlName(IntervalEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class IntervalEntity {
    public static final String TABLE_NAME = "intervals";
    public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

    public static final String FIELD_BOOK = "book";
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
    private final String book;

    @PartitionKey(2)
    @CqlName(FIELD_INTERVAL_START_DATE)
    private final LocalDate startDate;

    @ClusteringColumn(1)
    @CqlName(FIELD_CRAWLER_NAME)
    private final String crawlerName;

    @ClusteringColumn(2)
    @CqlName(FIELD_CRAWLER_VERSION)
    private final String crawlerVersion;

    @ClusteringColumn(3)
    @CqlName(FIELD_CRAWLER_TYPE)
    private final String crawlerType;

    @ClusteringColumn(4)
    @CqlName(FIELD_INTERVAL_START_TIME)
    private final LocalTime startTime;

    @CqlName(FIELD_INTERVAL_END_DATE)
    private final LocalDate endDate;

    @CqlName(FIELD_INTERVAL_END_TIME)
    private final LocalTime endTime;

    @CqlName(FIELD_INTERVAL_LAST_UPDATE_DATE)
    private final LocalDate lastUpdateDate;

    @CqlName(FIELD_INTERVAL_LAST_UPDATE_TIME)
    private final LocalTime lastUpdateTime;

    @CqlName(FIELD_RECOVERY_STATE_JSON)
    private final String recoveryState;

    @CqlName(FIELD_INTERVAL_PROCESSED)
    private final boolean processed;

    public IntervalEntity(String book, LocalDate startDate, String crawlerName, String crawlerVersion, String crawlerType, LocalTime startTime, LocalTime endTime, LocalDate lastUpdateDate, LocalDate endDate, LocalTime lastUpdateTime, String recoveryState, boolean processed) {
        this.book = book;
        this.startDate = startDate;
        this.crawlerName = crawlerName;
        this.crawlerVersion = crawlerVersion;
        this.crawlerType = crawlerType;
        this.startTime = startTime;
        this.endDate = endDate;
        this.endTime = endTime;
        this.lastUpdateDate = lastUpdateDate;
        this.lastUpdateTime = lastUpdateTime;
        this.recoveryState = recoveryState;
        this.processed = processed;
    }

    public IntervalEntity(Interval interval, PageId pageId) {
        this.startTime = LocalTime.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET));
        this.endTime = LocalTime.from(interval.getEndTime().atOffset(TIMEZONE_OFFSET));
        this.startDate = LocalDate.from(interval.getStartTime().atOffset(TIMEZONE_OFFSET));
        this.endDate = LocalDate.from(interval.getEndTime().atOffset(TIMEZONE_OFFSET));
        this.lastUpdateTime = LocalTime.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET));
        this.lastUpdateDate = LocalDate.from(interval.getLastUpdateDateTime().atOffset(TIMEZONE_OFFSET));
        this.recoveryState = interval.getRecoveryState();
        this.book = pageId.getBookId().getName();
        this.crawlerName = interval.getCrawlerName();
        this.crawlerVersion = interval.getCrawlerVersion();
        this.crawlerType = interval.getCrawlerType();
        this.processed = interval.isProcessed();
    }

    public String getBook() {
        return book;
    }
    public LocalDate getStartDate() {
        return startDate;
    }
    public LocalTime getStartTime() {
        return startTime;
    }
    public LocalTime getEndTime() {
        return endTime;
    }
    public LocalDate getLastUpdateDate() {
        return lastUpdateDate;
    }
    public LocalDate getEndDate() {
        return endDate;
    }
    public LocalTime getLastUpdateTime() {
        return lastUpdateTime;
    }
    public String getRecoveryState() {
        return recoveryState;
    }
    public String getCrawlerName() {
        return crawlerName;
    }
    public String getCrawlerVersion() {
        return crawlerVersion;
    }
    public String getCrawlerType() {
        return crawlerType;
    }
    public boolean isProcessed() {
        return processed;
    }

    public Interval asInterval() throws IOException {
        return Interval.builder().startTime(Instant.from(LocalDateTime.of(startDate, startTime).atOffset(TIMEZONE_OFFSET)))
                .endTime(Instant.from(LocalDateTime.of(endDate, endTime).atOffset(TIMEZONE_OFFSET)))
                .recoveryState(recoveryState)
                .lastUpdateTime(Instant.from(LocalDateTime.of(lastUpdateDate, lastUpdateTime).atOffset(TIMEZONE_OFFSET)))
                .crawlerName(crawlerName).crawlerVersion(crawlerVersion).crawlerType(crawlerType)
                .processed(processed).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IntervalEntity)) return false;
        IntervalEntity that = (IntervalEntity) o;

        return isProcessed() == that.isProcessed()
                && Objects.equals(getBook(), that.getBook())
                && Objects.equals(getStartDate(), that.getStartDate())
                && Objects.equals(getCrawlerName(), that.getCrawlerName())
                && Objects.equals(getCrawlerVersion(), that.getCrawlerVersion())
                && Objects.equals(getCrawlerType(), that.getCrawlerType())
                && Objects.equals(getStartTime(), that.getStartTime())
                && Objects.equals(getEndDate(), that.getEndDate())
                && Objects.equals(getEndTime(), that.getEndTime())
                && Objects.equals(getLastUpdateDate(), that.getLastUpdateDate())
                && Objects.equals(getLastUpdateTime(), that.getLastUpdateTime())
                && Objects.equals(getRecoveryState(), that.getRecoveryState());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBook(),
                getStartDate(),
                getCrawlerName(),
                getCrawlerVersion(),
                getCrawlerType(),
                getStartTime(),
                getEndDate(),
                getEndTime(),
                getLastUpdateDate(),
                getLastUpdateTime(),
                getRecoveryState(),
                isProcessed());
    }

    private static IntervalEntity build (IntervalEntityBuilder builder) {
        return new IntervalEntity(
                builder.book,
                builder.startDate,
                builder.crawlerName,
                builder.crawlerVersion,
                builder.crawlerType,
                builder.startTime,
                builder.endTime,
                builder.lastUpdateDate,
                builder.endDate,
                builder.lastUpdateTime,
                builder.recoveryState,
                builder.processed);
    }

    public static IntervalEntityBuilder builder () {
        return new IntervalEntityBuilder();
    }

    public static class IntervalEntityBuilder {

        private String book;
        private LocalDate startDate;
        private String crawlerName;
        private String crawlerVersion;
        private String crawlerType;
        private LocalTime startTime;
        private LocalDate endDate;
        private LocalTime endTime;
        private LocalDate lastUpdateDate;
        private LocalTime lastUpdateTime;
        private String recoveryState;
        private boolean processed;

        private IntervalEntityBuilder () {
        }

        public IntervalEntityBuilder setBook (String book) {
            this.book = book;
            return this;
        }

        public IntervalEntityBuilder setStartDate (LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        public IntervalEntityBuilder setCrawlerName (String crawlerName) {
            this.crawlerName = crawlerName;
            return this;
        }

        public IntervalEntityBuilder setCrawlerVersion (String crawlerVersion) {
            this.crawlerVersion = crawlerVersion;
            return this;
        }

        public IntervalEntityBuilder setCrawlerType (String crawlerType) {
            this.crawlerType = crawlerType;
            return this;
        }

        public IntervalEntityBuilder setStartTime (LocalTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public IntervalEntityBuilder setEndDate (LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }

        public IntervalEntityBuilder setEndTime (LocalTime endTime) {
            this.endTime = endTime;
            return this;
        }

        public IntervalEntityBuilder setLastUpdateDate (LocalDate lastUpdateDate) {
            this.lastUpdateDate = lastUpdateDate;
            return this;
        }

        public IntervalEntityBuilder setLastUpdateTime (LocalTime lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public IntervalEntityBuilder setRecoveryState (String recoveryState) {
            this.recoveryState = recoveryState;
            return this;
        }

        public IntervalEntityBuilder setProcessed (boolean processed) {
            this.processed = processed;
            return this;
        }

        public IntervalEntity build () {
            return IntervalEntity.build(this);
        }

    }
}
