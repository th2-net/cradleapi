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

package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;



public class SessionStatisticsBatchInserter {

    private final CqlSession session;
    private final PreparedStatement insertStatement;

    public SessionStatisticsBatchInserter(MapperContext context, EntityHelper<SessionStatisticsEntity> helper) {
        this.session = context.getSession();
        this.insertStatement = session.prepare(helper.insert().build());
    }

    public CompletableFuture<AsyncResultSet> insert(Collection<SessionStatisticsEntity> sessionBatch, Function<BatchStatementBuilder, BatchStatementBuilder> attributes) {

        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
        for (SessionStatisticsEntity entity: sessionBatch) {
            BoundStatementBuilder statementBuilder = insertStatement.boundStatementBuilder()
                    .setString(SessionStatisticsEntity.FIELD_BOOK, entity.getBook())
                    .setString(SessionStatisticsEntity.FIELD_PAGE, entity.getPage())
                    .setByte(SessionStatisticsEntity.FIELD_RECORD_TYPE, entity.getRecordType())
                    .setByte(SessionStatisticsEntity.FIELD_FRAME_TYPE, entity.getFrameType())
                    .setInstant(SessionStatisticsEntity.FIELD_FRAME_START, entity.getFrameStart())
                    .setString(SessionStatisticsEntity.FIELD_SESSION, entity.getSession());
            batchBuilder.addStatement(statementBuilder.build());
        }

        batchBuilder = attributes.apply(batchBuilder);
        return session.executeAsync(batchBuilder.build()).toCompletableFuture();
    }
}