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
                .setString(FIELD_PAGE, groupedMessageBatch.getPage())
                .setString(FIELD_ALIAS_GROUP, groupedMessageBatch.getGroup())
                .setString(FIELD_SESSION_ALIAS, groupedMessageBatch.getSessionAlias())
                .setString(FIELD_DIRECTION, groupedMessageBatch.getDirection())

                .setLocalDate(FIELD_MESSAGE_DATE, groupedMessageBatch.getMessageDate())
                .setLocalTime(FIELD_MESSAGE_TIME, groupedMessageBatch.getMessageTime())
                .setLong(FIELD_SEQUENCE, groupedMessageBatch.getSequence())

                .setLocalDate(FIELD_LAST_MESSAGE_DATE, groupedMessageBatch.getLastMessageDate())
                .setLocalTime(FIELD_LAST_MESSAGE_TIME, groupedMessageBatch.getLastMessageTime())
                .setLong(FIELD_LAST_SEQUENCE, groupedMessageBatch.getLastSequence())
                .setInt(FIELD_MESSAGE_COUNT, groupedMessageBatch.getMessageCount())
                .setBoolean(FIELD_COMPRESSED, groupedMessageBatch.isCompressed())
                .setSet(FIELD_LABELS, groupedMessageBatch.getLabels(), String.class)
                .setByteBuffer(FIELD_CONTENT, groupedMessageBatch.getContent())
                .setInstant(FIELD_REC_DATE, Instant.now());


        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}
