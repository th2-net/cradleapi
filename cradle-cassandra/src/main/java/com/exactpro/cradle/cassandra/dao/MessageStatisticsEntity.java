/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
public class MessageStatisticsEntity {

    private String sessionAlias;
    private String direction;
    private Byte frameType;
    private Instant frameStart;
    private Long entityCount;
    private Long entitySize;

    public MessageStatisticsEntity() {
    }

    public CounterSample toCounterSample () {
        return new CounterSample(FrameType.from(frameType), frameStart, new Counter(entityCount, entitySize));
    }

    @PartitionKey(0)
    @CqlName(SESSION_ALIAS)
    public String getSessionAlias() {
        return sessionAlias;
    }

    public void setSessionAlias(String sessionAlias) {
        this.sessionAlias = sessionAlias;
    }

    @PartitionKey(1)
    @CqlName(DIRECTION)
    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    @PartitionKey(2)
    @CqlName(FRAME_TYPE)
    public Byte getFrameType() {
        return frameType;
    }

    public void setFrameType(Byte frameType) {
        this.frameType = frameType;
    }

    @ClusteringColumn(3)
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