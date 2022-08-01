package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface EventBatchMaxDurationOperator {


    @Query("INSERT into ${qualifiedTableId} (" + INSTANCE_ID + ", " + START_DATE + ", " + MAX_BATCH_DURATION + ") "
            + "VALUES (:uuid, :startDate, :maxBatchDuration) "
            + "IF NOT EXISTS")
    CompletableFuture<Void> writeMaxDuration(UUID uuid, LocalDate startDate, long maxBatchDuration, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);


    @Query("UPDATE ${qualifiedTableId} SET " + MAX_BATCH_DURATION + "= :maxBatchDuration "
            + "WHERE " + INSTANCE_ID + "=:uuid AND " + START_DATE + "= :startDate "
            + "IF " + MAX_BATCH_DURATION + "<:duration")
    void updateMaxDuration(UUID uuid, LocalDate startDate, Long duration, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Select
   EventBatchMaxDurationEntity getMaxDuration(UUID uuid, LocalDate localDate, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
