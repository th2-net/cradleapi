package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class EventBatchMaxLengthEntity {

    @PartitionKey(0)
    @CqlName(INSTANCE_ID)
    private UUID instanceId;

    @PartitionKey(1)
    @CqlName(START_DATE)
    private LocalDate startDate;

    @CqlName(MAX_BATCH_LENGTH)
    private long maxBatchLength;

    public EventBatchMaxLengthEntity() {

    }

    public EventBatchMaxLengthEntity(UUID instanceId, LocalDate startDate, long maxBatchLength) {
        this.instanceId = instanceId;
        this.startDate = startDate;
        this.maxBatchLength = maxBatchLength;
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

    public long getMaxBatchLength() {
        return maxBatchLength;
    }

    public void setMaxBatchLength(long maxBatchLength) {
        this.maxBatchLength = maxBatchLength;
    }
}
