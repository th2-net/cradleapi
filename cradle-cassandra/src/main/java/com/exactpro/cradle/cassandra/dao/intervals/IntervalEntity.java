package com.exactpro.cradle.cassandra.dao.intervals;

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

    @PartitionKey(1)
    @CqlName(INTERVAL_ID)
    private String id;

    @PartitionKey(2)
    @CqlName(CRAWLER_TYPE)
    private String crawlerType;

    @CqlName(INTERVAL_LAST_UPDATE_DATE)
    private LocalDate lastUpdateDate;

    @CqlName(INTERVAL_LAST_UPDATE_TIME)
    private LocalTime lastUpdateTime;

    @CqlName(RECOVERY_STATE_JSON)
    private String recoveryStateJson;

    @CqlName(HEALED_EVENT_IDS)
    private Set<String> healedEventsIds;

    private LocalTime startTime;
    private LocalTime endTime;
    private LocalDate date;

    public IntervalEntity()
    {
    }

    public IntervalEntity(Interval interval, UUID instanceId)
    {
        this.id = interval.getId();
        this.startTime = interval.getStartTime();
        this.endTime = interval.getEndTime();
        this.date = interval.getDate();
        this.lastUpdateTime = interval.getLastUpdateTime();
        this.lastUpdateDate = interval.getLastUpdateDate();
        this.crawlerType = interval.getCrawlerType();
        this.recoveryStateJson = interval.getRecoveryState().convertToJson();
        this.instanceId = instanceId;
        this.healedEventsIds = new HashSet<>();
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

    public LocalDate getLastUpdateDate() { return lastUpdateDate; }

    public void setLastUpdateDate(LocalDate lastUpdateDate) { this.lastUpdateDate = lastUpdateDate; }

    public LocalTime getLastUpdateTime() { return lastUpdateTime; }

    public void setLastUpdateTime(LocalTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }

    public String getRecoveryStateJson() { return recoveryStateJson; }

    public void setRecoveryStateJson(String recoveryStateJson) { this.recoveryStateJson = recoveryStateJson; }

    public String getCrawlerType() { return crawlerType; }

    public void setCrawlerType(String crawlerType) { this.crawlerType = crawlerType; }

    public Set<String> getHealedEventsIds() { return healedEventsIds; }

    public void setHealedEventsIds(Set<String> healedEventsIds) { this.healedEventsIds = healedEventsIds; }

    public Interval asInterval() throws IOException {
        return new Interval(this.id, startTime, endTime, date, RecoveryState.getMAPPER().readValue(recoveryStateJson, RecoveryState.class),
                this.getLastUpdateDate(), this.getLastUpdateTime(), this.crawlerType);
    }
}
