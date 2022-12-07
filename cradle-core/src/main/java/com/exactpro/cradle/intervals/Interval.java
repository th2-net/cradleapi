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

package com.exactpro.cradle.intervals;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CompressionUtils;

import java.time.*;
import java.util.Objects;

public class Interval {

    private BookId bookId;
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;
    private String recoveryState;
    private LocalDateTime lastUpdateDateTime;
    private String crawlerName;
    private String crawlerVersion;
    private String crawlerType;
    private boolean processed;

    public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

    private Interval(BookId bookId,
                     LocalDateTime startDateTime,
                     LocalDateTime endDateTime,
                     String recoveryState,
                     LocalDateTime lastUpdateDateTime,
                     String crawlerName,
                     String crawlerVersion,
                     String crawlerType,
                     boolean processed) {
        this.bookId = bookId;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.recoveryState = recoveryState;
        this.lastUpdateDateTime = lastUpdateDateTime;
        this.crawlerName = crawlerName;
        this.crawlerVersion = crawlerVersion;
        this.crawlerType = crawlerType;
        this.processed = processed;
    }

    public BookId getBookId() {
        return bookId;
    }

    public Instant getStartTime() { return startDateTime.toInstant(TIMEZONE_OFFSET); }

    public Instant getEndTime() { return endDateTime.toInstant(TIMEZONE_OFFSET); }

    public String getRecoveryState() { return recoveryState; }

    public Instant getLastUpdateDateTime() { return lastUpdateDateTime.toInstant(TIMEZONE_OFFSET); }

    public String getCrawlerName() { return crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    public String getCrawlerType() { return crawlerType; }

    public boolean isProcessed() { return processed; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("Interval{").append(CompressionUtils.EOL)
                .append("startDate=").append(startDateTime.toLocalDate()).append(",").append(CompressionUtils.EOL)
                .append("startTime=").append(startDateTime.toLocalTime()).append(",").append(CompressionUtils.EOL)
                .append("endDate=").append(endDateTime.toLocalDate()).append(",").append(CompressionUtils.EOL)
                .append("endTime=").append(endDateTime.toLocalTime()).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateDate=").append(lastUpdateDateTime.toLocalDate()).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateTime=").append(lastUpdateDateTime.toLocalTime()).append(",").append(CompressionUtils.EOL)
                .append("crawlerName=").append(crawlerName).append(",").append(CompressionUtils.EOL)
                .append("crawlerVersion=").append(crawlerVersion).append(",").append(CompressionUtils.EOL)
                .append("crawlerType=").append(crawlerType).append(",").append(CompressionUtils.EOL)
                .append("processed=").append(processed).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                .append("}").toString();
    }

    private static Interval build (IntervalBuilder builder) {
        return new Interval(
                builder.bookId,
                builder.startDateTime,
                builder.endDateTime,
                builder.recoveryState,
                builder.lastUpdateDateTime,
                builder.crawlerName,
                builder.crawlerVersion,
                builder.crawlerType,
                builder.processed);
    }

    public static IntervalBuilder builder () {
        return new IntervalBuilder();
    }

    public static IntervalBuilder builder (Interval original) {
        return builder().setStartDateTime(original.startDateTime)
                .setEndDateTime(original.endDateTime)
                .setCrawlerName(original.crawlerName)
                .setCrawlerVersion(original.crawlerVersion)
                .setCrawlerType(original.crawlerType)
                .setRecoveryState(original.recoveryState)
                .setLastUpdateDateTime(original.lastUpdateDateTime)
                .setProcessed(original.processed);
    }

    public static class IntervalBuilder {
        private BookId bookId;
        private LocalDateTime startDateTime;
        private LocalDateTime endDateTime;
        private String recoveryState;
        private LocalDateTime lastUpdateDateTime;
        private String crawlerName;
        private String crawlerVersion;
        private String crawlerType;
        private boolean processed;

        public IntervalBuilder () {
        }

        public IntervalBuilder setBookId (BookId bookId) {
            this.bookId = bookId;
            return this;
        }

        public IntervalBuilder setStartDateTime (LocalDateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        public IntervalBuilder setEndDateTime (LocalDateTime endDateTime) {
            this.endDateTime = endDateTime;
            return this;
        }

        public IntervalBuilder setRecoveryState (String recoveryState) {
            this.recoveryState = recoveryState;
            return this;
        }

        public IntervalBuilder setLastUpdateDateTime (LocalDateTime lastUpdateDateTime) {
            this.lastUpdateDateTime = lastUpdateDateTime;
            return this;
        }

        public IntervalBuilder setCrawlerName (String crawlerName) {
            this.crawlerName = crawlerName;
            return this;
        }

        public IntervalBuilder setCrawlerVersion (String crawlerVersion) {
            this.crawlerVersion = crawlerVersion;
            return this;
        }

        public IntervalBuilder setCrawlerType (String crawlerType) {
            this.crawlerType = crawlerType;
            return this;
        }

        public IntervalBuilder setProcessed (boolean processed) {
            this.processed = processed;
            return this;
        }

        public Interval build () {
            Objects.requireNonNull(bookId, "'bookId' parameter");
            Objects.requireNonNull(lastUpdateDateTime, "'lastUpdateDateTime' parameter");

            return Interval.build(this);
        }
    }
}
