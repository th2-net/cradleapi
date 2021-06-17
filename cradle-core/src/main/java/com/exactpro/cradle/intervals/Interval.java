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

public class Interval {
    private String id;
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;
    private RecoveryState recoveryState;
    private LocalDateTime lastUpdateDateTime;
    private String crawlerName;
    private String crawlerVersion;
    private boolean processed;

    public static IntervalBuilder builder() { return new IntervalBuilder(); }

    public String getId() { return id; }

    public void setId(String id) { this.id = id; }

    public LocalDateTime getStartDateTime() { return startDateTime; }

    public void setStartDateTime(LocalDateTime startDateTime) { this.startDateTime = startDateTime; }

    public LocalDateTime getEndDateTime() { return endDateTime; }

    public void setEndDateTime(LocalDateTime endDateTime) { this.endDateTime = endDateTime; }

    public RecoveryState getRecoveryState() { return recoveryState; }

    public void setRecoveryState(RecoveryState recoveryState) { this.recoveryState = recoveryState; }

    public LocalDateTime getLastUpdateDateTime() { return lastUpdateDateTime; }

    public void setLastUpdateDateTime(LocalDateTime lastUpdateDateTime) { this.lastUpdateDateTime = lastUpdateDateTime; }

    public String getCrawlerName() { return crawlerName; }

    public void setCrawlerName(String crawlerName) { this.crawlerName = crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    public void setCrawlerVersion(String crawlerVersion) { this.crawlerVersion = crawlerVersion; }

    public boolean isProcessed() { return processed; }

    public void setProcessed(boolean processed) { this.processed = processed; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("Interval{").append(CompressionUtils.EOL)
                .append("id=").append(id).append(",").append(CompressionUtils.EOL)
                .append("startDate=").append(startDateTime.toLocalDate()).append(",").append(CompressionUtils.EOL)
                .append("startTime=").append(startDateTime.toLocalTime()).append(",").append(CompressionUtils.EOL)
                .append("endDate=").append(endDateTime.toLocalDate()).append(",").append(CompressionUtils.EOL)
                .append("endTime=").append(endDateTime.toLocalTime()).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateDate=").append(lastUpdateDateTime.toLocalDate()).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateTime=").append(lastUpdateDateTime.toLocalTime()).append(",").append(CompressionUtils.EOL)
                .append("crawlerName=").append(crawlerName).append(",").append(CompressionUtils.EOL)
                .append("crawlerVersion=").append(crawlerVersion).append(",").append(CompressionUtils.EOL)
                .append("processed=").append(processed).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                .append("}").toString();
    }
}
