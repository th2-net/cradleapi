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


package com.exactpro.cradle.healing;

import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

public class HealingInterval
{
    private final LocalTime startTime;
    private final LocalTime endTime;
    private final LocalDate date;
    private final RecoveryState recoveryState;
    private final LocalDate lastUpdateDate;
    private final LocalTime lastUpdateTime;
    private final String crawlerType;
    private Set<String> healedEventsIds;

    private final ZoneOffset timezoneOffset = ZoneOffset.UTC;

    public HealingInterval(Instant start, Instant end, RecoveryState recoveryState,
                           LocalDate lastUpdateDate, LocalTime lastUpdateTime,
                           String crawlerType) throws CradleStorageException
    {

        LocalDateTime fromDateTime = LocalDateTime.ofInstant(start, timezoneOffset),
                toDateTime = LocalDateTime.ofInstant(end, timezoneOffset);
        checkTimeBoundaries(fromDateTime, toDateTime, start, end);

        this.startTime = fromDateTime.toLocalTime();
        this.endTime = toDateTime.toLocalTime();
        this.date = fromDateTime.toLocalDate();
        this.recoveryState = recoveryState;
        this.lastUpdateDate = lastUpdateDate;
        this.lastUpdateTime = lastUpdateTime;
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet<>();
    }

    public HealingInterval(Instant start, Instant end,
                           RecoveryState recoveryState, String crawlerType) throws CradleStorageException
    {
        LocalDateTime fromDateTime = LocalDateTime.ofInstant(start, timezoneOffset),
                toDateTime = LocalDateTime.ofInstant(end, timezoneOffset);
        checkTimeBoundaries(fromDateTime, toDateTime, start, end);

        this.startTime = fromDateTime.toLocalTime();
        this.endTime = toDateTime.toLocalTime();
        this.date = fromDateTime.toLocalDate();
        this.recoveryState = recoveryState;
        this.lastUpdateDate = LocalDate.now();
        this.lastUpdateTime = LocalTime.now();
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet<>();
    }

    public HealingInterval(LocalTime startTime, LocalTime endTime, LocalDate date, RecoveryState recoveryState, LocalDate lastUpdateDate, LocalTime lastUpdateTime, String crawlerType) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.date = date;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = lastUpdateDate;
        this.lastUpdateTime = lastUpdateTime;
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet();
    }

    public HealingInterval(LocalTime startTime, LocalTime endTime, LocalDate date, RecoveryState recoveryState, String crawlerType) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.date = date;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = LocalDate.now();
        this.lastUpdateTime = LocalTime.now();
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet();
    }

    private void checkTimeBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime, Instant originalFrom, Instant originalTo)
            throws CradleStorageException
    {
        LocalDate fromDate = fromDateTime.toLocalDate(),
                toDate = toDateTime.toLocalDate();
        if (!fromDate.equals(toDate))
            throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+originalFrom+"' and '"+originalTo+"'");
    }

    public LocalTime getStartTime() { return startTime; }

    public LocalTime getEndTime() { return endTime; }

    public LocalDate getDate() { return date; }

    public RecoveryState getRecoveryState() { return recoveryState; }

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public String getCrawlerType() { return crawlerType; }

    public Set<String> getHealedEventsIds() { return healedEventsIds; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("HealingInterval{").append(CompressionUtils.EOL)
                .append("date=").append(date).append(",").append(CompressionUtils.EOL)
                .append("startTime=").append(startTime).append(",").append(CompressionUtils.EOL)
                .append("endTime=").append(endTime).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateDate=").append(lastUpdateDate).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateTime=").append(lastUpdateTime).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                .append("}").toString();
    }
}
