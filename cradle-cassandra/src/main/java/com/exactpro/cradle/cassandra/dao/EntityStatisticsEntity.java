package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.FrameType;

import java.time.Instant;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class EntityStatisticsEntity {

    private Byte entityType;
    private Byte frameType;
    private Instant frameStart;
    private Long entityCount;
    private Long entitySize;

    public EntityStatisticsEntity() {

    }

    public CounterSample toCounterSample () {
        return new CounterSample(FrameType.from(frameType), frameStart, new Counter(entityCount, entitySize));
    }

    @PartitionKey(0)
    @CqlName(ENTITY_TYPE)
    public Byte getEntityType() {
        return entityType;
    }

    public void setEntityType(Byte entityType) {
        this.entityType = entityType;
    }

    @PartitionKey(1)
    @CqlName(FRAME_TYPE)
    public Byte getFrameType() {
        return frameType;
    }

    public void setFrameType(Byte frameType) {
        this.frameType = frameType;
    }

    @ClusteringColumn(2)
    @CqlName(FRAME_START)
    public Instant getFrameStart() {
        return frameStart;
    }


    public void setFrameStart(Instant frameStart) {
        this.frameStart = frameStart;
    }

    @CqlName(ENTITY_COUNT)
    public Long getEntityCount() {
        return entityCount;
    }

    public void setEntityCount(Long entityCount) {
        this.entityCount = entityCount;
    }

    @CqlName(ENTITY_SIZE)
    public Long getEntitySize() {
        return entitySize;
    }

    public void setEntitySize(Long entitySize) {
        this.entitySize = entitySize;
    }
}