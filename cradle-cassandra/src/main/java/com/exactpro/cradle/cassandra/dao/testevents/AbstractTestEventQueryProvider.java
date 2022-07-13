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
import com.exactpro.cradle.cassandra.utils.SelectArguments;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public abstract class AbstractTestEventQueryProvider<V> {

    private final CqlSession session;
    private final EntityHelper<V> helper;

    private final Map<SelectArguments, PreparedStatement> statementCache;


    public AbstractTestEventQueryProvider(MapperContext context, EntityHelper<V> helper) {
        this.session = context.getSession();
        this.helper = helper;
        statementCache = new ConcurrentHashMap<>();
    }


    private Select selectStart (boolean includeContent) {
        Select select = QueryBuilder.selectFrom(helper.getKeyspaceId(), helper.getTableId())
                .column(INSTANCE_ID)
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

        if (includeContent)
            select = select .column(CONTENT)
                            .column(COMPRESSED);

        return select;
    }


    private Select addConditions(Select select, String idFrom, String parentId, Order order) {
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


    public PreparedStatement getPreparedStatement(boolean includeContent, String idFrom, String parentId, Order order){
        SelectArguments arguments = new SelectArguments(includeContent, idFrom, parentId, order);
        PreparedStatement preparedStatement = statementCache.computeIfAbsent(arguments, key -> {
             Select select = selectStart(key.getIncludeContent());
             select = addConditions(select, key.getIdFrom(), key.getParentId(), key.getOrder());
             return session.prepare(select.build());
        });
        return preparedStatement;
    }

    protected BoundStatement bindParameters(
            PreparedStatement preparedStatement,
            UUID instanceId,
            LocalDate startDate,
            LocalTime timeFrom,
            String idFrom,
            LocalTime timeTo,
            String parentId,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes)
    {
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

        BoundStatement boundStatement = builder.build();
        return boundStatement;
    }

    protected CompletableFuture<MappedAsyncPagingIterable<V>> execute(BoundStatement statement) {
        return session.executeAsync(statement)
                .thenApply(r -> r.map(helper::get))
                .toCompletableFuture();
    }
}
