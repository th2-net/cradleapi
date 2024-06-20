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

import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.*;

public class MessageBatchInserter {
    private final CqlSession session;
    private final PreparedStatement insertStatement;

    public MessageBatchInserter(MapperContext context, EntityHelper<MessageBatchEntity> helper) {
        this.session = context.getSession();
        this.insertStatement = session.prepare(helper.insert().build());
    }

    public CompletableFuture<AsyncResultSet> insert(MessageBatchEntity messageBatch, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        BoundStatementBuilder builder = insertStatement.boundStatementBuilder()
                .setString(FIELD_BOOK, messageBatch.getBook())
                .setString(FIELD_PAGE, messageBatch.getPage())
                .setString(FIELD_SESSION_ALIAS, messageBatch.getSessionAlias())
                .setString(FIELD_DIRECTION, messageBatch.getDirection())

                .setLocalDate(FIELD_FIRST_MESSAGE_DATE, messageBatch.getFirstMessageDate())
                .setLocalTime(FIELD_FIRST_MESSAGE_TIME, messageBatch.getFirstMessageTime())
                .setLong(FIELD_SEQUENCE, messageBatch.getSequence())

                .setLocalDate(FIELD_LAST_MESSAGE_DATE, messageBatch.getLastMessageDate())
                .setLocalTime(FIELD_LAST_MESSAGE_TIME, messageBatch.getLastMessageTime())
                .setLong(FIELD_LAST_SEQUENCE, messageBatch.getLastSequence())
                .setInt(FIELD_MESSAGE_COUNT, messageBatch.getMessageCount())
                .setBoolean(FIELD_COMPRESSED, messageBatch.isCompressed())
                .setByteBuffer(FIELD_CONTENT, messageBatch.getContent())
                .setInstant(FIELD_REC_DATE, Instant.now())
                .setInt(FIELD_CONTENT_SIZE, messageBatch.getContentSize())
                .setInt(FIELD_UNCOMPRESSED_CONTENT_SIZE, messageBatch.getUncompressedContentSize());

        if (messageBatch.getLabels() != null) {
            builder = builder.setSet(FIELD_LABELS, messageBatch.getLabels(), String.class);
        }

        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}