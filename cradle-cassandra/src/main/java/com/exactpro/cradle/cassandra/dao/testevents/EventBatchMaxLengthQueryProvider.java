package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class EventBatchMaxLengthQueryProvider{

    private final CqlSession session;
    private final EntityHelper<EventBatchMaxLengthEntity> helper;


    public EventBatchMaxLengthQueryProvider(MapperContext context, EntityHelper<EventBatchMaxLengthEntity> helper) {
        this.session = context.getSession();
        this.helper = helper;
    }



    CompletableFuture<EventBatchMaxLengthEntity> writeMaxLength (UUID uuid, LocalDate startDate, long maxBatchLength, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {

        BoundStatement insertStatement = getInsertBoundStatement(uuid, startDate, maxBatchLength, attributes);
        BoundStatement updateStatement = getUpdateBoundStatement(uuid, startDate, maxBatchLength, attributes);

        BatchStatement batchStatement = BatchStatement.newInstance(DefaultBatchType.LOGGED);

        batchStatement.add(insertStatement);
        batchStatement.add(updateStatement);

        return session.executeAsync(batchStatement).toCompletableFuture()
                .thenApply(r -> r.map(helper::get))
                .thenApply(asyncPagingIterable -> {
                    EventBatchMaxLengthEntity entity = null;

                    for (EventBatchMaxLengthEntity el : asyncPagingIterable.currentPage()) {
                        if (entity == null || entity.getMaxBatchLength() < el.getMaxBatchLength()) {
                            entity = el;
                        }
                    }

                    return entity;
                });
    }

    private BoundStatement getInsertBoundStatement(UUID uuid, LocalDate startDate, long maxBatchLength, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {

        Insert insertInto = QueryBuilder.insertInto(helper.getKeyspaceId(), helper.getTableId())
                .value(INSTANCE_ID, QueryBuilder.bindMarker(INSTANCE_ID))
                .value(START_DATE, QueryBuilder.bindMarker(START_DATE))
                .value(MAX_BATCH_LENGTH, QueryBuilder.bindMarker(MAX_BATCH_LENGTH))
                .ifNotExists();

        PreparedStatement preparedStatement = session.prepare(insertInto.build());

        BoundStatementBuilder builder =  preparedStatement.boundStatementBuilder();
        attributes.apply(builder);

        builder = builder.setUuid(INSTANCE_ID, uuid)
                .setLocalDate(START_DATE, startDate)
                .setLong(MAX_BATCH_LENGTH, maxBatchLength);

        return builder.build();
    }

    private BoundStatement getUpdateBoundStatement (UUID uuid, LocalDate startDate, long maxBatchLength, Function<BoundStatementBuilder, BoundStatementBuilder> attributes) {
        var update = QueryBuilder.update(helper.getKeyspaceId(), helper.getTableId())
                .setColumn(MAX_BATCH_LENGTH, QueryBuilder.bindMarker(MAX_BATCH_LENGTH))
                .whereColumn(INSTANCE_ID).isEqualTo(QueryBuilder.bindMarker(INSTANCE_ID))
                .whereColumn(START_DATE).isEqualTo(QueryBuilder.bindMarker(START_DATE))
                .ifColumn(MAX_BATCH_LENGTH).isLessThan(QueryBuilder.bindMarker());

        PreparedStatement preparedStatement = session.prepare(update.build());

        BoundStatementBuilder builder =  preparedStatement.boundStatementBuilder();
        attributes.apply(builder);

        builder = builder.setUuid(INSTANCE_ID, uuid)
                .setLocalDate(START_DATE, startDate)
                .setLong(MAX_BATCH_LENGTH, maxBatchLength);

        return builder.build();
    }
}
