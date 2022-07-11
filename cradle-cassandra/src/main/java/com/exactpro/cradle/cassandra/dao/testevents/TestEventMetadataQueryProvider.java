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
        Select select = getSelect(true, idFrom, null, order);

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
        Select select = getSelect(true, idFrom, parentId, null);

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
