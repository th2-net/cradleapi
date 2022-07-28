package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface EventBatchMaxLengthOperator {

    @Query("INSERT into ${qualifiedTableId} (" + INSTANCE_ID + ", " + START_DATE + ", " + MAX_BATCH_LENGTH + ")"
            + "VALUES (:uuid, :startDate, :maxBatchLength)"
            + "IF NOT EXISTS")
    EventBatchMaxLengthEntity writeMaxLength(UUID uuid, LocalDate startDate, long maxBatchLength, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("UPDATE ${qualifiedTableId} SET " + MAX_BATCH_LENGTH + "=:maxBatchLength"
            + "WHERE " + INSTANCE_ID + "=:uuid AND " + START_DATE + "=:startDate"
            + "IF " + MAX_BATCH_LENGTH + "<:maxBatchLength")
    EventBatchMaxLengthEntity updateMaxLength(UUID uuid, LocalDate startDate, long maxBatchLength, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Select
    EventBatchMaxLengthEntity getMaxLength (UUID uuid, LocalDate localDate, LocalTime startTime, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
