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

import com.exactpro.cradle.utils.CompressionUtils;

import java.time.*;
import java.util.Objects;

public class Interval {
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;
    private String recoveryState;
    private LocalDateTime lastUpdateDateTime;
    private String crawlerName;
    private String crawlerVersion;
    private String crawlerType;
    private boolean processed;

    public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

    public static IntervalBuilder builder() { return new IntervalBuilder(); }

    public Instant getStartTime() { return startDateTime.toInstant(TIMEZONE_OFFSET); }

    public void setStartTime(Instant startTime) { this.startDateTime = LocalDateTime.ofInstant(startTime, TIMEZONE_OFFSET); }

    public Instant getEndTime() { return endDateTime.toInstant(TIMEZONE_OFFSET); }

    public void setEndTime(Instant endTime) { this.endDateTime = LocalDateTime.ofInstant(endTime, TIMEZONE_OFFSET); }

    public String getRecoveryState() { return recoveryState; }

    public void setRecoveryState(String recoveryState) { this.recoveryState = recoveryState; }

    public Instant getLastUpdateDateTime() { return lastUpdateDateTime.toInstant(TIMEZONE_OFFSET); }

    public void setLastUpdateDateTime(Instant lastUpdateTime) { this.lastUpdateDateTime = LocalDateTime.ofInstant(lastUpdateTime, TIMEZONE_OFFSET); }

    public String getCrawlerName() { return crawlerName; }

    public void setCrawlerName(String crawlerName) { this.crawlerName = crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    public void setCrawlerVersion(String crawlerVersion) { this.crawlerVersion = crawlerVersion; }

    public String getCrawlerType() { return crawlerType; }

    public void setCrawlerType(String crawlerType) { this.crawlerType = crawlerType; }

    public boolean isProcessed() { return processed; }

    public void setProcessed(boolean processed) { this.processed = processed; }

    public static Interval copyWith(Interval original, String recoveryStateJson, LocalDateTime lastUpdateDateTime, boolean processed) {
        Objects.requireNonNull(lastUpdateDateTime, "'lastUpdateDateTime' parameter");
        Interval interval = new Interval();
        interval.setStartTime(original.getStartTime());
        interval.setEndTime(original.getEndTime());
        interval.setCrawlerName(original.getCrawlerName());
        interval.setCrawlerVersion(original.getCrawlerVersion());
        interval.setCrawlerType(original.getCrawlerType());

        interval.setRecoveryState(recoveryStateJson);
        interval.lastUpdateDateTime = lastUpdateDateTime;
        interval.setProcessed(processed);
        return interval;
    }

    public static Interval copyWith(Interval original, String recoveryStateJson, Instant lastUpdateDateTime, boolean processed) {
        return copyWith(original, recoveryStateJson, LocalDateTime.ofInstant(lastUpdateDateTime, TIMEZONE_OFFSET), processed);
    }

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
}
