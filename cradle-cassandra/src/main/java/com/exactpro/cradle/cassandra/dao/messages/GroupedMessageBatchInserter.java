/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.messages;

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

import static com.exactpro.cradle.cassandra.dao.CradleEntity.FIELD_COMPRESSED;
import static com.exactpro.cradle.cassandra.dao.CradleEntity.FIELD_CONTENT;
import static com.exactpro.cradle.cassandra.dao.CradleEntity.FIELD_LABELS;
import static com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity.*;

public class GroupedMessageBatchInserter {
    private final CqlSession session;
    private final PreparedStatement insertStatement;

    public GroupedMessageBatchInserter(MapperContext context, EntityHelper<GroupedMessageBatchEntity> helper) {
        this.session = context.getSession();
        this.insertStatement = session.prepare(helper.insert().build());
    }

    public CompletableFuture<AsyncResultSet> insert(GroupedMessageBatchEntity groupedMessageBatch, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        BoundStatementBuilder builder = insertStatement.boundStatementBuilder()
                .setString(FIELD_BOOK, groupedMessageBatch.getBook())
                .setString(FIELD_PAGE, groupedMessageBatch.getPage())
                .setString(FIELD_ALIAS_GROUP, groupedMessageBatch.getGroup())

                .setLocalDate(FIELD_FIRST_MESSAGE_DATE, groupedMessageBatch.getFirstMessageDate())
                .setLocalTime(FIELD_FIRST_MESSAGE_TIME, groupedMessageBatch.getFirstMessageTime())

                .setLocalDate(FIELD_LAST_MESSAGE_DATE, groupedMessageBatch.getLastMessageDate())
                .setLocalTime(FIELD_LAST_MESSAGE_TIME, groupedMessageBatch.getLastMessageTime())
                .setInt(FIELD_MESSAGE_COUNT, groupedMessageBatch.getMessageCount())
                .setBoolean(FIELD_COMPRESSED, groupedMessageBatch.isCompressed());

        if (groupedMessageBatch.getLabels() != null) {
            builder = builder.setSet(FIELD_LABELS, groupedMessageBatch.getLabels(), String.class);
        }

        builder = builder.setByteBuffer(FIELD_CONTENT, groupedMessageBatch.getContent())
                .setInstant(FIELD_REC_DATE, Instant.now())
                .setInt(FIELD_CONTENT_SIZE, groupedMessageBatch.getContentSize())
                .setInt(FIELD_UNCOMPRESSED_CONTENT_SIZE, groupedMessageBatch.getUncompressedContentSize());

        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}