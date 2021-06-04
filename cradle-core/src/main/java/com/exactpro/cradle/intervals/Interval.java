package com.exactpro.cradle.intervals;

import com.exactpro.cradle.utils.CompressionUtils;

import java.time.*;
import java.util.HashSet;
import java.util.Set;

public class Interval {
    private final String id;
    private final LocalTime startTime;
    private final LocalDate date;
    private final RecoveryState recoveryState;
    private final LocalDate lastUpdateDate;
    private final LocalTime lastUpdateTime;
    private final String crawlerName;
    private final String crawlerVersion;

    public Interval(String id, LocalTime startTime, LocalDate date, RecoveryState recoveryState,
                    LocalDate lastUpdateDate, LocalTime lastUpdateTime, String crawlerName, String crawlerVersion)
    {
        this.id = id;
        this.startTime = startTime;
        this.date = date;
        this.recoveryState = recoveryState;
        this.lastUpdateDate = lastUpdateDate;
        this.lastUpdateTime = lastUpdateTime;
        this.crawlerName = crawlerName;
        this.crawlerVersion = crawlerVersion;
    }

    public String getId() { return id; }

    public LocalTime getStartTime() { return startTime; }

    public LocalDate getDate() { return date; }

    public RecoveryState getRecoveryState() { return recoveryState; }

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public String getCrawlerName() { return crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("Interval{").append(CompressionUtils.EOL)
                .append("id=").append(id).append(",").append(CompressionUtils.EOL)
                .append("date=").append(date).append(",").append(CompressionUtils.EOL)
                .append("startTime=").append(startTime).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateDate=").append(lastUpdateDate).append(",").append(CompressionUtils.EOL)
                .append("lastUpdateTime=").append(lastUpdateTime).append(",").append(CompressionUtils.EOL)
                .append(recoveryState.toString()).append(CompressionUtils.EOL)
                .append("}").toString();
    }
}
