package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.time.LocalDate;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class EventBatchMaxDurationEntity {

    @PartitionKey(0)
    @CqlName(INSTANCE_ID)
    private UUID instanceId;

    @PartitionKey(1)
    @CqlName(START_DATE)
    private LocalDate startDate;

    @CqlName(MAX_BATCH_DURATION)
    private long maxBatchDuration;

    public EventBatchMaxDurationEntity() {

    }

    public EventBatchMaxDurationEntity(UUID instanceId, LocalDate startDate, long maxBatchDuration) {
        this.instanceId = instanceId;
        this.startDate = startDate;
        this.maxBatchDuration = maxBatchDuration;
    }

    public UUID getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(UUID instanceId) {
        this.instanceId = instanceId;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    public long getMaxBatchDuration() {
        return maxBatchDuration;
    }

    public void setMaxBatchDuration(long maxBatchDuration) {
        this.maxBatchDuration = maxBatchDuration;
    }
}
