package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.Counter;

import java.time.Instant;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class StatisticsEntity {

    @PartitionKey(0)
    @CqlName(SESSION_ALIAS)
    private String sessionAlias;

    @PartitionKey(1)
    @CqlName(DIRECTION)
    private String direction;

    @PartitionKey(2)
    @CqlName(ENTITY_TYPE)
    private Byte entityType;

    @PartitionKey(3)
    @CqlName(FRAME_TYPE)
    private Byte frameType;

    @ClusteringColumn(1)
    @CqlName(FRAME_START)
    private Instant instant;

    @CqlName(ENTITY_COUNT)
    private Long entityCount;

    @CqlName(ENTITY_SIZE)
    private Long entitySize;

    public StatisticsEntity() {

    }

    public Counter toCounter () {
        return new Counter(entityCount, entitySize);
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public void setSessionAlias(String sessionAlias) {
        this.sessionAlias = sessionAlias;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public Byte getEntityType() {
        return entityType;
    }

    public void setEntityType(Byte entityType) {
        this.entityType = entityType;
    }

    public Byte getFrameType() {
        return frameType;
    }

    public void setFrameType(Byte frameType) {
        this.frameType = frameType;
    }

    public Instant getInstant() {
        return instant;
    }

    public void setInstant(Instant instant) {
        this.instant = instant;
    }

    public Long getEntityCount() {
        return entityCount;
    }

    public void setEntityCount(Long entityCount) {
        this.entityCount = entityCount;
    }

    public Long getEntitySize() {
        return entitySize;
    }

    public void setEntitySize(Long entitySize) {
        this.entitySize = entitySize;
    }
}