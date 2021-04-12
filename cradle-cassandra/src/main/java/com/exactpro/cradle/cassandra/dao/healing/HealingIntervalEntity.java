package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.healing.HealingInterval;
import com.exactpro.cradle.healing.RecoveryState;

import java.time.Instant;

import static com.exactpro.cradle.cassandra.StorageConstants.HEALED_EVENTS_NUMBER;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_MAX_LENGTH;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_START_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.RECOVERY_STATE_ID;

public class HealingIntervalEntity
{
    @PartitionKey(0)
    @CqlName(HEALING_INTERVAL_ID)
    private String healingIntervalId;

    @PartitionKey(1)
    @CqlName(HEALING_INTERVAL_START_TIME)
    private Instant startTime;

    @PartitionKey(2)
    @CqlName(HEALING_INTERVAL_MAX_LENGTH)
    private long maxLength;

    @PartitionKey(3)
    @CqlName(RECOVERY_STATE_ID)
    private String recoveryStateId;

    @PartitionKey(4)
    @CqlName(HEALED_EVENTS_NUMBER)
    private long healedEventsNumber;

    public HealingIntervalEntity()
    {
    }

    public HealingIntervalEntity(HealingInterval interval)
    {
        this.healingIntervalId = interval.getId();
        this.startTime = interval.getStartTime();
        this.maxLength = interval.getMaxLength();
        this.recoveryStateId = interval.getRecoveryState().getId();
        this.healedEventsNumber = interval.getRecoveryState().getHealedEventsNumber();
    }

    public String getHealingIntervalId() { return healingIntervalId; }

    public void setHealingIntervalId(String healingIntervalId) { this.healingIntervalId = healingIntervalId; }

    public Instant getStartTime() { return startTime; }

    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public long getMaxLength() { return maxLength; }

    public void setMaxLength(long maxLength) { this.maxLength = maxLength; }

    public HealingInterval asHealingInterval() {
        return new HealingInterval(this.healingIntervalId, this.startTime, this.maxLength, new RecoveryState(recoveryStateId, healedEventsNumber));
    }
}
