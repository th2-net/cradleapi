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
                .setSet(FIELD_LABELS, messageBatch.getLabels(), String.class)
                .setByteBuffer(FIELD_CONTENT, messageBatch.getContent())
                .setInstant(FIELD_REC_DATE, Instant.now());


        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}
