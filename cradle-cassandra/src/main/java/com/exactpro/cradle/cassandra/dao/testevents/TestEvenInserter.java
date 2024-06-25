/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.*;

public class TestEvenInserter {

    private final CqlSession session;
    private final PreparedStatement insertStatement;

    public TestEvenInserter(MapperContext context, EntityHelper<TestEventEntity> helper) {
        this.session = context.getSession();
        this.insertStatement = session.prepare(helper.insert().build());
    }

    public CompletableFuture<AsyncResultSet> insert(TestEventEntity testEvent, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        BoundStatementBuilder builder = insertStatement.boundStatementBuilder()
                .setString(FIELD_BOOK, testEvent.getBook())
                .setString(FIELD_PAGE, testEvent.getPage())
                .setString(FIELD_SCOPE, testEvent.getScope())

                .setLocalDate(FIELD_START_DATE, testEvent.getStartDate())
                .setLocalTime(FIELD_START_TIME, testEvent.getStartTime())
                .setString(FIELD_ID, testEvent.getId())

                .setString(FIELD_NAME, testEvent.getName())
                .setBoolean(FIELD_SUCCESS, testEvent.isSuccess())
                .setBoolean(FIELD_ROOT, testEvent.isRoot())
                .setString(FIELD_PARENT_ID, testEvent.getParentId())
                .setBoolean(FIELD_EVENT_BATCH, testEvent.isEventBatch())
                .setInt(FIELD_EVENT_COUNT, testEvent.getEventCount())
                .setLocalDate(FIELD_END_DATE, testEvent.getEndDate())
                .setLocalTime(FIELD_END_TIME, testEvent.getEndTime())
                .setBoolean(FIELD_COMPRESSED, testEvent.isCompressed())
                .setByteBuffer(FIELD_CONTENT, testEvent.getContent())
                .setInstant(FIELD_REC_DATE, Instant.now())
                .setInt(FIELD_CONTENT_SIZE, testEvent.getContentSize())
                .setInt(FIELD_UNCOMPRESSED_CONTENT_SIZE, testEvent.getUncompressedContentSize());

        // We skip setting null value to columns to avoid tombstone creation.
        if (testEvent.getType() != null) {
            builder = builder.setString(FIELD_TYPE, testEvent.getType());
        }

        if (testEvent.getMessages() != null) {
            builder = builder.setByteBuffer(FIELD_MESSAGES, testEvent.getMessages());
        }

        if (testEvent.getLabels() != null) {
            builder = builder.setSet(FIELD_LABELS, testEvent.getLabels(), String.class);
        }

        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}