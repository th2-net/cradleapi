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

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Order;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class TestEventMetadataQueryProvider extends AbstractTestEventQueryProvider<TestEventMetadataEntity> {

    public TestEventMetadataQueryProvider(MapperContext context, EntityHelper<TestEventMetadataEntity> helper) {
        super(context, helper);
    }



    public CompletableFuture<MappedAsyncPagingIterable<TestEventMetadataEntity>> getTestEventsMetadata(
            UUID instanceId,
            LocalDate startDate,
            LocalTime timeFrom,
            String idFrom,
            LocalTime timeTo,
            Order order,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes)

    {
        Select select = selectStart(true);
        select = addConditions(select, idFrom, null, order);

        BoundStatement statement = bindParameters(  select,
                instanceId,
                startDate,
                timeFrom,
                idFrom,
                timeTo,
                null,
                attributes);

        return execute(statement);
    }

    public CompletableFuture<MappedAsyncPagingIterable<TestEventMetadataEntity>> getTestEventsMetadata(
            UUID instanceId,
            LocalDate startDate,
            LocalTime timeFrom,
            String idFrom,
            LocalTime timeTo,
            String parentId,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes)

    {
        Select select = selectStart(true);
        select = addConditions(select, idFrom, parentId, null);

        BoundStatement statement = bindParameters(  select,
                instanceId,
                startDate,
                timeFrom,
                idFrom,
                timeTo,
                parentId,
                attributes);

        return execute(statement);
    }
}