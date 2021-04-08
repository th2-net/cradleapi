package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.healing.HealingInterval;

import java.time.Instant;

import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_END_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_START_TIME;

public class HealingIntervalEntity
{
    @PartitionKey(0)
    @CqlName(HEALING_INTERVAL_ID)
    private String healingIntervalId;

    @PartitionKey(1)
    @CqlName(HEALING_INTERVAL_START_TIME)
    private Instant startTime;

    @PartitionKey(2)
    @CqlName(HEALING_INTERVAL_END_TIME)
    private Instant endTime;

    public HealingIntervalEntity()
    {
    }

    public HealingIntervalEntity(HealingInterval interval)
    {
        this.healingIntervalId = interval.getId();
        this.startTime = interval.getStartTime();
        this.endTime = interval.getEndTime();
    }

    public String getHealingIntervalId() { return healingIntervalId; }

    public void setHealingIntervalId(String healingIntervalId) { this.healingIntervalId = healingIntervalId; }

    public Instant getStartTime() { return startTime; }

    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }

    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public HealingInterval asHealingInterval() {
        return new HealingInterval(this.healingIntervalId, this.startTime, this.endTime);
    }
}
