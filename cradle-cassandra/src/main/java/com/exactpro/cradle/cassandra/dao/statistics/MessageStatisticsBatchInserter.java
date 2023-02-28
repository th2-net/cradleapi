/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.dao.statistics.MessageStatisticsEntity.*;


public class MessageStatisticsBatchInserter {

    private final CqlSession session;
    private final PreparedStatement updateStatement;

    public MessageStatisticsBatchInserter(MapperContext context, EntityHelper<MessageStatisticsEntity> helper) {
        this.session = context.getSession();
        this.updateStatement =  session.prepare(QueryBuilder.update(helper.getKeyspaceId(), helper.getTableId())
                                                .increment(FIELD_ENTITY_COUNT, bindMarker(FIELD_ENTITY_COUNT))
                                                .increment(FIELD_ENTITY_SIZE, bindMarker(FIELD_ENTITY_SIZE))
                                                .whereColumn(FIELD_BOOK).isEqualTo(bindMarker(FIELD_BOOK))
                                                .whereColumn(FIELD_PAGE).isEqualTo(bindMarker(FIELD_PAGE))
                                                .whereColumn(FIELD_SESSION_ALIAS).isEqualTo(bindMarker(FIELD_SESSION_ALIAS))
                                                .whereColumn(FIELD_DIRECTION).isEqualTo(bindMarker(FIELD_DIRECTION))
                                                .whereColumn(FIELD_FRAME_TYPE).isEqualTo(bindMarker(FIELD_FRAME_TYPE))
                                                .whereColumn(FIELD_FRAME_START).isEqualTo(bindMarker(FIELD_FRAME_START))
                                                .build());
    }

    public CompletableFuture<AsyncResultSet> update(Collection<MessageStatisticsEntity> counters, Function<BatchStatementBuilder, BatchStatementBuilder> attributes) {

        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.COUNTER);
        for (MessageStatisticsEntity counter: counters) {
            BoundStatementBuilder statementBuilder = updateStatement.boundStatementBuilder()
                    .setString(FIELD_BOOK, counter.getBook())
                    .setString(FIELD_PAGE, counter.getPage())
                    .setString(FIELD_SESSION_ALIAS, counter.getSessionAlias())
                    .setString(FIELD_DIRECTION, counter.getDirection())
                    .setByte(FIELD_FRAME_TYPE, counter.getFrameType())
                    .setInstant(FIELD_FRAME_START, counter.getFrameStart())
                    .setLong(FIELD_ENTITY_COUNT, counter.getEntityCount())
                    .setLong(FIELD_ENTITY_SIZE, counter.getEntitySize());
            batchBuilder.addStatement(statementBuilder.build());
        }

        batchBuilder = attributes.apply(batchBuilder);
        return session.executeAsync(batchBuilder.build()).toCompletableFuture();
    }
}