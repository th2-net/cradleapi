/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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


    private Select selectStart (boolean includeContent) {
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


    private Select addConditions(Select select, String idFrom, String idTo, String parentId, Order order) {
        select = select
                .whereColumn(INSTANCE_ID).isEqualTo(bindMarker(INSTANCE_ID))
                .whereColumn(START_DATE).isEqualTo(bindMarker(START_DATE));

        if (idFrom == null)
            select = select.whereColumn(START_TIME).isGreaterThanOrEqualTo(bindMarker(START_TIME + "_FROM"));
        else
            select = select.whereColumns(START_TIME, ID).isGreaterThanOrEqualTo(tuple(bindMarker(START_TIME + "_FROM"), bindMarker(ID + "_FROM")));

        if (idTo == null)
            select = select.whereColumn(START_TIME).isLessThan(bindMarker(START_TIME + "_TO"));
        else
            select = select.whereColumns(START_TIME, ID).isLessThanOrEqualTo(tuple(bindMarker(START_TIME + "_TO"), bindMarker(ID + "_TO")));



        if (parentId != null)
            select = select.whereColumn(PARENT_ID).isEqualTo(bindMarker(PARENT_ID));

        if (order != null && parentId == null) {
            ClusteringOrder orderBy = order.equals(Order.DIRECT) ? ClusteringOrder.ASC : ClusteringOrder.DESC;
            select = select .orderBy(START_TIME, orderBy)
                            .orderBy(ID, orderBy);
        }

        return select;
    }


    public PreparedStatement getPreparedStatement(boolean includeContent, String idFrom, String idTo, String parentId, Order order){
        Select select = selectStart(includeContent);
        select = addConditions(select, idFrom, idTo, parentId, order);
        return session.prepare(select.build());
    }

    protected BoundStatement bindParameters(
            PreparedStatement preparedStatement,
            UUID instanceId,
            LocalDate startDate,
            LocalTime timeFrom,
            String idFrom,
            String idTo,
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
            builder = builder.setString(ID + "_FROM", idFrom);

        if (idTo != null)
            builder = builder.setString(ID + "_TO", idTo);

        if (parentId != null)
            builder = builder.setString(PARENT_ID, parentId);

        return builder.build();
    }

    protected CompletableFuture<MappedAsyncPagingIterable<V>> execute(BoundStatement statement) {
        return session.executeAsync(statement)
                .thenApply(r -> r.map(source -> helper.get(source, false)))
                .toCompletableFuture();
    }
}
