package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Order;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class TestEventQueryProvider {

    private final CqlSession session;
    private final EntityHelper<TestEventEntity> helper;
    private final Select selectStart;

    public TestEventQueryProvider(MapperContext context, EntityHelper<TestEventEntity> helper)
    {
        this.session = context.getSession();
        this.helper = helper;
        this.selectStart = helper.selectStart();
    }

    CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getTestEventsFromId(UUID instanceId, LocalDate startDate, String fromId,
                                                                                      LocalTime timeFrom, LocalTime timeTo, Order order,
                                                                                      Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        Select select = selectStart;
        select = select.whereColumn(INSTANCE_ID).isEqualTo(bindMarker())
                .whereColumn(START_DATE).isEqualTo(bindMarker())
                .whereColumns(START_TIME, ID).isGreaterThan(tuple(bindMarker(), bindMarker()))
                .whereColumn(START_TIME).isLessThan(bindMarker())
                .orderBy(START_TIME, order.equals(Order.DIRECT) ? ClusteringOrder.ASC : ClusteringOrder.DESC);

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

    CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> getTestEventsFromId(UUID instanceId, LocalDate startDate, String parentId,
                                                                                      String fromId, LocalTime timeFrom, LocalTime timeTo, Order order,
                                                                                      Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        Select select = selectStart;
        select = select.whereColumn(INSTANCE_ID).isEqualTo(bindMarker())
                .whereColumn(START_DATE).isEqualTo(bindMarker())
                .whereColumns(PARENT_ID).isEqualTo(bindMarker())
                .whereColumns(START_TIME, ID).isGreaterThan(tuple(bindMarker(), bindMarker()))
                .whereColumn(START_TIME).isLessThan(bindMarker())
                .orderBy(START_TIME, order.equals(Order.DIRECT) ? ClusteringOrder.ASC : ClusteringOrder.DESC);

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
}
