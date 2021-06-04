package com.exactpro.cradle.cassandra.dao.intervals;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.intervals.Interval;
import com.exactpro.cradle.intervals.RecoveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class IntervalEntity {
    private static final Logger logger = LoggerFactory.getLogger(IntervalEntity.class);

    @PartitionKey(0)
    @CqlName(INSTANCE_ID)
    private UUID instanceId;

    @ClusteringColumn(0)
    @CqlName(INTERVAL_ID)
    private String id;

    @ClusteringColumn(1)
    @CqlName(INTERVAL_DATE)
    private LocalDate date;

    @ClusteringColumn(2)
    @CqlName(CRAWLER_NAME)
    private String crawlerName;

    @ClusteringColumn(3)
    @CqlName(CRAWLER_VERSION)
    private String crawlerVersion;

    @ClusteringColumn(4)
    @CqlName(INTERVAL_START_TIME)
    private LocalTime startTime;

    @CqlName(INTERVAL_LAST_UPDATE_DATE)
    private LocalDate lastUpdateDate;

    @CqlName(INTERVAL_LAST_UPDATE_TIME)
    private LocalTime lastUpdateTime;

    @CqlName(RECOVERY_STATE_JSON)
    private String recoveryStateJson;

    public IntervalEntity()
    {
    }

    public IntervalEntity(Interval interval, UUID instanceId)
    {
        this.id = interval.getId();
        this.startTime = interval.getStartTime();
        this.date = interval.getDate();
        this.lastUpdateTime = interval.getLastUpdateTime();
        this.lastUpdateDate = interval.getLastUpdateDate();
        this.recoveryStateJson = interval.getRecoveryState().convertToJson();
        this.instanceId = instanceId;
        this.crawlerName = interval.getCrawlerName();
        this.crawlerVersion = interval.getCrawlerVersion();
    }

    public UUID getInstanceId()
    {
        return instanceId;
    }

    public void setInstanceId(UUID instanceId)
    {
        this.instanceId = instanceId;
    }

    public String getId() { return id; }

    public void setId(String id) { this.id = id; }

    public LocalDate getDate() { return date; }

    public void setDate(LocalDate date) { this.date = date; }

    public LocalTime getStartTime() { return startTime; }

    public void setStartTime(LocalTime startTime) { this.startTime = startTime; }

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public void setLastUpdateDate(LocalDate lastUpdateDate) { this.lastUpdateDate = lastUpdateDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public void setLastUpdateTime(LocalTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }

    public String getRecoveryStateJson() { return recoveryStateJson; }

    public void setRecoveryStateJson(String recoveryStateJson) { this.recoveryStateJson = recoveryStateJson; }

    public String getCrawlerName() { return crawlerName; }

    public void setCrawlerName(String crawlerName) { this.crawlerName = crawlerName; }

    public String getCrawlerVersion() { return crawlerVersion; }

    public void setCrawlerVersion(String crawlerVersion) { this.crawlerVersion = crawlerVersion; }

    public Interval asInterval() throws IOException {
        return new Interval(id, startTime, date, RecoveryState.getMAPPER().readValue(recoveryStateJson, RecoveryState.class),
                lastUpdateDate, lastUpdateTime, crawlerName, crawlerVersion);
    }
}
