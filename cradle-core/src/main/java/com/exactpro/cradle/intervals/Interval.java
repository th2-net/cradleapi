package com.exactpro.cradle.intervals;

import com.exactpro.cradle.utils.CompressionUtils;

import java.time.*;
import java.util.HashSet;
import java.util.Set;

public class Interval {
    private final String id;
    private final LocalTime startTime;
    private final LocalTime endTime;
    private final LocalDate date;
    private final RecoveryState recoveryState;
    private final LocalDate lastUpdateDate;
    private final LocalTime lastUpdateTime;
    private final String crawlerType;
    private final Set<String> healedEventsIds;

    public Interval(String id, LocalTime startTime, LocalTime endTime, LocalDate date, RecoveryState recoveryState, LocalDate lastUpdateDate, LocalTime lastUpdateTime, String crawlerType)
    {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.date = date;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = lastUpdateDate;
        this.lastUpdateTime = lastUpdateTime;
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet<>();
    }

    public Interval(String id, LocalTime startTime, LocalTime endTime, LocalDate date, RecoveryState recoveryState, String crawlerType)
    {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.date = date;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = LocalDate.now();
        this.lastUpdateTime = LocalTime.now();
        this.crawlerType = crawlerType;
        this.healedEventsIds = new HashSet<>();
    }

    public String getId() { return id; }

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
                .append("Interval{").append(CompressionUtils.EOL)
                .append("id=").append(id).append(",").append(CompressionUtils.EOL)
                .append("date=").append(date).append(",").append(CompressionUtils.EOL)
                .append("startTime=").append(startTime).append(",").append(CompressionUtils.EOL)
                .append("endTime=").append(endTime).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateDate=").append(lastUpdateDate).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateTime=").append(lastUpdateTime).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                //.append("healedEventsIds={").
                .append("}").toString();
    }
}
