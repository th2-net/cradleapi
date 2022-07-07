package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
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

public abstract class AbstractTestEventQueryProvider<V> {

    private final CqlSession session;
    private final EntityHelper<V> helper;

    public AbstractTestEventQueryProvider(MapperContext context, EntityHelper<V> helper) {
        this.session = context.getSession();
        this.helper = helper;
    }


    protected Select selectStart (boolean includeContent) {
        Select select = QueryBuilder.selectFrom(helper.getKeyspaceId(), helper.getTableId())
                .column(INSTANCE_ID)
                .column(START_DATE)
                .column(START_TIME)
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

        if (includeContent)
            select = select .column(CONTENT)
                            .column(COMPRESSED)
                            .column(MESSAGE_IDS);
        return select;
    }


    protected Select addConditions(Select select, String idFrom, String parentId, Order order) {
        select = select
                .whereColumn(INSTANCE_ID).isEqualTo(bindMarker(INSTANCE_ID))
                .whereColumn(START_DATE).isEqualTo(bindMarker(START_DATE));

        if (idFrom == null)
            select = select.whereColumn(START_TIME).isGreaterThanOrEqualTo(bindMarker(START_TIME + "_FROM"));
        else
            select = select.whereColumns(START_TIME, ID).isGreaterThan(tuple(bindMarker(START_TIME + "_FROM"), bindMarker(ID)));


        if (parentId != null)
            select = select.whereColumns(PARENT_ID).isEqualTo(bindMarker(PARENT_ID));

        select = select.whereColumn(START_TIME).isLessThan(bindMarker(START_TIME + "_TO"));

        if (order != null && parentId == null) {
            ClusteringOrder orderBy = order.equals(Order.DIRECT) ? ClusteringOrder.ASC : ClusteringOrder.DESC;
            select = select .orderBy(START_TIME, orderBy)
                            .orderBy(START_TIME, orderBy);
        }

        return select;
    }


    protected BoundStatement bindParameters(
            Select select,
            UUID instanceId,
            LocalDate startDate,
            LocalTime timeFrom,
            String idFrom,
            LocalTime timeTo,
            String parentId,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
    {
        PreparedStatement preparedStatement = session.prepare(select.build());
        BoundStatementBuilder builder =  preparedStatement.boundStatementBuilder();
        attributes.apply(builder);

        builder = builder   .setUuid(INSTANCE_ID, instanceId)
                            .setLocalDate(START_DATE, startDate)
                            .setLocalTime(START_TIME + "_FROM", timeFrom)
                            .setLocalTime(START_TIME + "_TO", timeTo);

        if (idFrom != null)
            builder = builder.setString(ID, idFrom);

        if (parentId != null)
            builder = builder.setString(PARENT_ID, parentId);

        return builder.build();
    }

    protected CompletableFuture<MappedAsyncPagingIterable<V>> execute(BoundStatement statement) {
        return session.executeAsync(statement)
                .thenApply(r -> r.map(helper::get))
                .toCompletableFuture();
    }
}
