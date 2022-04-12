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

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class TestEvenInserter {

    private final CqlSession session;
    private final PreparedStatement insertStatement;

    public TestEvenInserter(MapperContext context, EntityHelper<TestEventEntity> helper) {
        this.session = context.getSession();
        this.insertStatement = session.prepare(helper.insert().build());
    }

    public CompletableFuture<AsyncResultSet> insert(TestEventEntity testEvent, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        BoundStatementBuilder builder = insertStatement.boundStatementBuilder()
                .setString(PAGE, testEvent.getPage())
                .setString(SCOPE, testEvent.getScope())

                .setLocalDate(START_DATE, testEvent.getStartDate())
                .setLocalTime(START_TIME, testEvent.getStartTime())
                .setString(ID, testEvent.getId())

                .setString(NAME, testEvent.getName())
                .setString(TYPE, testEvent.getType())
                .setBoolean(SUCCESS, testEvent.isSuccess())
                .setBoolean(ROOT, testEvent.isRoot())
                .setString(PARENT_ID, testEvent.getParentId())
                .setBoolean(EVENT_BATCH, testEvent.isEventBatch())
                .setInt(EVENT_COUNT, testEvent.getEventCount())
                .setLocalDate(END_DATE, testEvent.getEndDate())
                .setLocalTime(END_TIME, testEvent.getEndTime())
                .setBoolean(COMPRESSED, testEvent.isCompressed())
                .setByteBuffer(MESSAGES, testEvent.getMessages())
                .setSet(LABELS, testEvent.getLabels(), String.class)
                .setByteBuffer(CONTENT, testEvent.getContent())
                .setInstant(REC_DATE, Instant.now());


        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}
