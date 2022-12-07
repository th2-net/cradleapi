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

    private final BookId bookId;
    private final Instant start;
    private final Instant end;
    private final Instant lastUpdate;
    private final String recoveryState;
    private final String crawlerName;
    private final String crawlerVersion;
    private final String crawlerType;
    private final boolean processed;

    public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

    private Interval(BookId bookId,
                     Instant start,
                     Instant end,
                     String recoveryState,
                     Instant lastUpdate,
                     String crawlerName,
                     String crawlerVersion,
                     String crawlerType,
                     boolean processed) {
        this.bookId = bookId;
        this.start = start;
        this.end = end;
        this.recoveryState = recoveryState;
        this.lastUpdate = lastUpdate;
        this.crawlerName = crawlerName;
        this.crawlerVersion = crawlerVersion;
        this.crawlerType = crawlerType;
        this.processed = processed;
    }

    public BookId getBookId() {
        return bookId;
    }

    public Instant getStart() {
        return start;
    }

    public Instant getEnd() {
        return end;
    }

    public Instant getLastUpdate() {
        return lastUpdate;
    }

    public String getRecoveryState() { return recoveryState; }

    public String getCrawlerName() { return crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    public String getCrawlerType() { return crawlerType; }

    public boolean isProcessed() { return processed; }

    @Override
    public String toString() {
        return "Interval{" +
                "bookId=" + bookId +
                ", start=" + start +
                ", end=" + end +
                ", lastUpdate=" + lastUpdate +
                ", recoveryState='" + recoveryState + '\'' +
                ", crawlerName='" + crawlerName + '\'' +
                ", crawlerVersion='" + crawlerVersion + '\'' +
                ", crawlerType='" + crawlerType + '\'' +
                ", processed=" + processed +
                '}';
    }

    private static Interval build (IntervalBuilder builder) {
        return new Interval(
                builder.bookId,
                builder.start,
                builder.end,
                builder.recoveryState,
                builder.lastUpdate,
                builder.crawlerName,
                builder.crawlerVersion,
                builder.crawlerType,
                builder.processed);
    }

    public static IntervalBuilder builder () {
        return new IntervalBuilder();
    }

    public static IntervalBuilder builder (Interval original) {
        return builder()
                .setBookId(original.bookId)
                .setStart(original.start)
                .setEnd(original.end)
                .setLastUpdate(original.lastUpdate)
                .setCrawlerName(original.crawlerName)
                .setCrawlerVersion(original.crawlerVersion)
                .setCrawlerType(original.crawlerType)
                .setRecoveryState(original.recoveryState)
                .setProcessed(original.processed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Interval)) return false;
        Interval interval = (Interval) o;
        return isProcessed() == interval.isProcessed()
                && getBookId().equals(interval.getBookId())
                && Objects.equals(getStart(), interval.getStart())
                && Objects.equals(getEnd(), interval.getEnd())
                && getLastUpdate().equals(interval.getLastUpdate())
                && Objects.equals(getRecoveryState(), interval.getRecoveryState())
                && Objects.equals(getCrawlerName(), interval.getCrawlerName())
                && Objects.equals(getCrawlerVersion(), interval.getCrawlerVersion())
                && Objects.equals(getCrawlerType(), interval.getCrawlerType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getBookId(),
                getStart(),
                getEnd(),
                getLastUpdate(),
                getRecoveryState(),
                getCrawlerName(),
                getCrawlerVersion(),
                getCrawlerType(),
                isProcessed());
    }

    public static class IntervalBuilder {
        private BookId bookId;
        private Instant start;
        private Instant end;
        private String recoveryState;
        private Instant lastUpdate;
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

        public IntervalBuilder setStart (Instant start) {
            this.start = start;
            return this;
        }

        public IntervalBuilder setEnd (Instant end) {
            this.end = end;
            return this;
        }

        public IntervalBuilder setLastUpdate (Instant lastUpdate) {
            this.lastUpdate = lastUpdate;
            return this;
        }

        public IntervalBuilder setRecoveryState (String recoveryState) {
            this.recoveryState = recoveryState;
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
            Objects.requireNonNull(lastUpdate, "'lastUpdateDate' parameter");

            return Interval.build(this);
        }
    }
}
