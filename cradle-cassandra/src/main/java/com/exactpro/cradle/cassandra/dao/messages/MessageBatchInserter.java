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

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class MessageBatchInserter {

    private final CqlSession session;
    private final PreparedStatement insertStatement;

    public MessageBatchInserter(MapperContext context, EntityHelper<MessageBatchEntity> helper) {
        this.session = context.getSession();
        this.insertStatement = session.prepare(helper.insert().build());
    }

    public CompletableFuture<AsyncResultSet> insert(MessageBatchEntity messageBatch, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        BoundStatementBuilder builder = insertStatement.boundStatementBuilder()
                .setString(PAGE, messageBatch.getPage())
                .setString(SESSION_ALIAS, messageBatch.getSessionAlias())
                .setString(DIRECTION, messageBatch.getDirection())

                .setLocalDate(MESSAGE_DATE, messageBatch.getMessageDate())
                .setLocalTime(MESSAGE_TIME, messageBatch.getMessageTime())
                .setLong(SEQUENCE, messageBatch.getSequence())

                .setLocalDate(LAST_MESSAGE_DATE, messageBatch.getLastMessageDate())
                .setLocalTime(LAST_MESSAGE_TIME, messageBatch.getLastMessageTime())
                .setLong(LAST_SEQUENCE, messageBatch.getLastSequence())
                .setInt(MESSAGE_COUNT, messageBatch.getMessageCount())
                .setBoolean(COMPRESSED, messageBatch.isCompressed())
                .setSet(LABELS, messageBatch.getLabels(), String.class)
                .setByteBuffer(CONTENT, messageBatch.getContent())
                .setInstant(REC_DATE, Instant.now());


        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}
