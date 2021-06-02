package com.exactpro.cradle.intervals;

import com.exactpro.cradle.utils.CompressionUtils;

import java.time.*;
import java.util.HashSet;
import java.util.Set;

public class Interval {
    private final String id;
    private final RecoveryState recoveryState;
    private final LocalDate lastUpdateDate;
    private final LocalTime lastUpdateTime;
    private final String crawlerType;
    private Set<String> healedEventsIds;

    private final ZoneOffset timezoneOffset = ZoneOffset.UTC;

    public Interval(String id, RecoveryState recoveryState, LocalDate lastUpdateDate, LocalTime lastUpdateTime, String crawlerType)
    {
        this.id = id;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = lastUpdateDate;
        this.lastUpdateTime = lastUpdateTime;
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet<>();
    }

    public Interval(String id, RecoveryState recoveryState, String crawlerType)
    {
        this.id = id;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = LocalDate.now();
        this.lastUpdateTime = LocalTime.now();
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet<>();
    }

    public String getId() { return id; }

    public RecoveryState getRecoveryState() { return recoveryState; }

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public String getCrawlerType() { return crawlerType; }

    public Set<String> getHealedEventsIds() { return healedEventsIds; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("Interval{").append(CompressionUtils.EOL)
                .append("lastUpdateDate=").append(lastUpdateDate).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateTime=").append(lastUpdateTime).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                .append("}").toString();
    }
}
