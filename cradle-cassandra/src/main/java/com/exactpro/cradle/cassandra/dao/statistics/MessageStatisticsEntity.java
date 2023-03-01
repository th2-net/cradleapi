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
package com.exactpro.cradle.cassandra.dao.statistics;

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.FrameType;

import java.time.Instant;
import java.util.Objects;

@Entity
@CqlName(MessageStatisticsEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class MessageStatisticsEntity {
    public static final String TABLE_NAME = "message_statistics";

    public static final String FIELD_BOOK = "book";
    public static final String FIELD_PAGE = "page";
    public static final String FIELD_SESSION_ALIAS = "session_alias";
    public static final String FIELD_DIRECTION = "direction";
    public static final String FIELD_FRAME_TYPE = "frame_type";
    public static final String FIELD_FRAME_START = "frame_start";
    public static final String FIELD_ENTITY_COUNT = "entity_count";
    public static final String FIELD_ENTITY_SIZE = "entity_size";

    @PartitionKey(0)
    @CqlName(FIELD_BOOK)
    private final String book;

    @PartitionKey(1)
    @CqlName(FIELD_PAGE)
    private final String page;

    @PartitionKey(2)
    @CqlName(FIELD_SESSION_ALIAS)
    private final String sessionAlias;

    @PartitionKey(3)
    @CqlName(FIELD_DIRECTION)
    private final String direction;

    @PartitionKey(4)
    @CqlName(FIELD_FRAME_TYPE)
    private final Byte frameType;

    @ClusteringColumn(5)
    @CqlName(FIELD_FRAME_START)
    private final Instant frameStart;

    @CqlName(FIELD_ENTITY_COUNT)
    private final Long entityCount;

    @CqlName(FIELD_ENTITY_SIZE)
    private final Long entitySize;

    public MessageStatisticsEntity(String book, String page, String sessionAlias, String direction, Byte frameType, Instant frameStart, Long entityCount, Long entitySize) {
        this.book = book;
        this.page = page;
        this.sessionAlias = sessionAlias;
        this.direction = direction;
        this.frameType = frameType;
        this.frameStart = frameStart;
        this.entityCount = entityCount;
        this.entitySize = entitySize;
    }

    public CounterSample toCounterSample () {
        return new CounterSample(FrameType.from(frameType), frameStart, new Counter(entityCount, entitySize));
    }

    public String getBook() {
        return book;
    }
    public String getPage() {
        return page;
    }
    public String getSessionAlias() {
        return sessionAlias;
    }
    public String getDirection() {
        return direction;
    }
    public Byte getFrameType() {
        return frameType;
    }
    public Instant getFrameStart() {
        return frameStart;
    }
    public Long getEntityCount() {
        return entityCount;
    }
    public Long getEntitySize() {
        return entitySize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageStatisticsEntity)) return false;
        MessageStatisticsEntity that = (MessageStatisticsEntity) o;

        return Objects.equals(getBook(), that.getBook())
                && Objects.equals(getPage(), that.getPage())
                && Objects.equals(getSessionAlias(), that.getSessionAlias())
                && Objects.equals(getDirection(), that.getDirection())
                && Objects.equals(getFrameType(), that.getFrameType())
                && Objects.equals(getFrameStart(), that.getFrameStart())
                && Objects.equals(getEntityCount(), that.getEntityCount())
                && Objects.equals(getEntitySize(), that.getEntitySize());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBook(),
                getPage(),
                getSessionAlias(),
                getDirection(),
                getFrameType(),
                getFrameStart(),
                getEntityCount(),
                getEntitySize());
    }
}