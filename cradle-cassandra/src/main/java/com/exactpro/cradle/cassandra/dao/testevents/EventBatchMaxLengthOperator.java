package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;

import java.time.LocalDate;
import java.util.UUID;
import java.util.function.Function;

@Dao
public interface EventBatchMaxLengthOperator {

    @QueryProvider(providerClass = EventBatchMaxLengthQueryProvider.class, entityHelpers = EventBatchMaxLengthEntity.class)
    EventBatchMaxLengthEntity writeMaxLength (UUID uuid, LocalDate startDate, long maxBatchLength, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
    @Select
    EventBatchMaxLengthEntity getMaxLength (UUID uuid, LocalDate localDate, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
