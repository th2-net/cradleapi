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
                .setString(FIELD_PAGE, testEvent.getPage())
                .setString(FIELD_SCOPE, testEvent.getScope())

                .setLocalDate(FIELD_START_DATE, testEvent.getStartDate())
                .setLocalTime(FIELD_START_TIME, testEvent.getStartTime())
                .setString(FIELD_ID, testEvent.getId())

                .setString(FIELD_NAME, testEvent.getName())
                .setString(FIELD_TYPE, testEvent.getType())
                .setBoolean(FIELD_SUCCESS, testEvent.isSuccess())
                .setBoolean(FIELD_ROOT, testEvent.isRoot())
                .setString(FIELD_PARENT_ID, testEvent.getParentId())
                .setBoolean(FIELD_EVENT_BATCH, testEvent.isEventBatch())
                .setInt(FIELD_EVENT_COUNT, testEvent.getEventCount())
                .setLocalDate(FIELD_END_DATE, testEvent.getEndDate())
                .setLocalTime(FIELD_END_TIME, testEvent.getEndTime())
                .setBoolean(FIELD_COMPRESSED, testEvent.isCompressed())
                .setByteBuffer(FIELD_MESSAGES, testEvent.getMessages())
                .setSet(FIELD_LABELS, testEvent.getLabels(), String.class)
                .setByteBuffer(FIELD_CONTENT, testEvent.getContent())
                .setInstant(FIELD_REC_DATE, Instant.now());


        attributes.apply(builder);
        BoundStatement statement = builder.build();
        return session.executeAsync(statement).toCompletableFuture();
    }
}
