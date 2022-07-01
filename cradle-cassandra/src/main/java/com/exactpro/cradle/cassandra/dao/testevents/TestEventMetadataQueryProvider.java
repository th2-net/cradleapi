package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class TestEventMetadataQueryProvider {

    private final CqlSession session;
    private final EntityHelper<TestEventMetadataEntity> helper;
    private final Select selectStart;

    public TestEventMetadataQueryProvider(MapperContext context, EntityHelper<TestEventMetadataEntity> helper)
    {
        this.session = context.getSession();
        this.helper = helper;
        this.selectStart = helper.selectStart();
    }

    private Select withMetadataColumns (Select select) {
        return select.column(INSTANCE_ID)
                .column(START_DATE)
                .column(START_DATE)
                .column(ID)
                .column(NAME)
                .column(TYPE)
                .column(EVENT_BATCH)
                .column(END_DATE)
                .column(END_TIME)
                .column(SUCCESS)
                .column(EVENT_COUNT)
                .column(EVENT_BATCH_METADATA)
                .column(ROOT)
                .column(PARENT_ID);
    }

    CompletableFuture<MappedAsyncPagingIterable<TestEventMetadataEntity>> getTestEventsFromIdMetadata (UUID instanceId, LocalDate startDate, String parentId,
                                                                                               String fromId, LocalTime timeFrom, LocalTime timeTo,
                                                                                               Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        Select select = withMetadataColumns(selectStart);

        select = select.whereColumn(INSTANCE_ID).isEqualTo(bindMarker())
                .whereColumn(START_DATE).isEqualTo(bindMarker())
                .whereColumns(PARENT_ID).isEqualTo(bindMarker())
                .whereColumns(START_TIME, ID).isGreaterThan(tuple(bindMarker(), bindMarker()))
                .whereColumn(START_TIME).isLessThan(bindMarker());

        PreparedStatement preparedStatement = session.prepare(select.build());
        BoundStatementBuilder builder =  preparedStatement.boundStatementBuilder();
        attributes.apply(builder);
        BoundStatement statement = builder.setUuid(0, instanceId)
                .setLocalDate(1, startDate)
                .setString(2, parentId)
                .setLocalTime(3, timeFrom)
                .setString(4, fromId)
                .setLocalTime(5, timeTo).build();

        return session.executeAsync(statement).thenApply(r -> r.map(helper::get)).toCompletableFuture();
    }

    CompletableFuture<MappedAsyncPagingIterable<TestEventMetadataEntity>> getTestEventsFromIdMetadata (UUID instanceId, LocalDate startDate,
                                                                                               String fromId, LocalTime timeFrom, LocalTime timeTo,
                                                                                               Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        Select select = withMetadataColumns(selectStart);

        select = select.whereColumn(INSTANCE_ID).isEqualTo(bindMarker())
                .whereColumn(START_DATE).isEqualTo(bindMarker())
                .whereColumns(START_TIME, ID).isGreaterThan(tuple(bindMarker(), bindMarker()))
                .whereColumn(START_TIME).isLessThan(bindMarker());

        PreparedStatement preparedStatement = session.prepare(select.build());
        BoundStatementBuilder builder =  preparedStatement.boundStatementBuilder();
        attributes.apply(builder);
        BoundStatement statement = builder.setUuid(0, instanceId)
                .setLocalDate(1, startDate)
                .setLocalTime(2, timeFrom)
                .setString(3, fromId)
                .setLocalTime(4, timeTo).build();

        return session.executeAsync(statement).thenApply(r -> r.map(helper::get)).toCompletableFuture();
    }

}
