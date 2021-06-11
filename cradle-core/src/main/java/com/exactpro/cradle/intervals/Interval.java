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
    private final String id;
    private final LocalTime startTime;
    private final LocalTime endTime;
    private final LocalDate startDate;
    private final LocalDate endDate;
    private final RecoveryState recoveryState;
    private final LocalDate lastUpdateDate;
    private final LocalTime lastUpdateTime;
    private final String crawlerName;
    private final String crawlerVersion;
    private final boolean processed;

    public Interval(String id, LocalTime startTime, LocalTime endTime, LocalDate startDate, LocalDate endDate, RecoveryState recoveryState,
                    LocalDate lastUpdateDate, LocalTime lastUpdateTime, String crawlerName, String crawlerVersion, boolean processed)
    {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startDate = startDate;
        this.endDate = endDate;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = lastUpdateDate;
        this.lastUpdateTime = lastUpdateTime;
        this.crawlerName = crawlerName;
        this.crawlerVersion = crawlerVersion;
        this.processed = processed;
    }

    public String getId() { return id; }

    public LocalTime getStartTime() { return startTime; }

    public LocalTime getEndTime() { return endTime; }

    public LocalDate getStartDate() { return startDate; }

    public LocalDate getEndDate() { return endDate; }

    public RecoveryState getRecoveryState() { return recoveryState; }

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public String getCrawlerName() { return crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    public boolean isProcessed() { return processed; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("Interval{").append(CompressionUtils.EOL)
                .append("id=").append(id).append(",").append(CompressionUtils.EOL)
                .append("date=").append(startDate).append(",").append(CompressionUtils.EOL)
                .append("startTime=").append(startTime).append(",").append(CompressionUtils.EOL)
                .append("endTime=").append(endTime).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateDate=").append(lastUpdateDate).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateTime=").append(lastUpdateTime).append(",").append(CompressionUtils.EOL)
                .append("crawlerName=").append(crawlerName).append(",").append(CompressionUtils.EOL)
                .append("crawlerVersion=").append(crawlerVersion).append(",").append(CompressionUtils.EOL)
                .append("processed=").append(processed).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                .append("}").toString();
    }
}
