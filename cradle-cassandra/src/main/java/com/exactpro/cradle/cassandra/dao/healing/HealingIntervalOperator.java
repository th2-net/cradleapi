package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Update;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

//TODO: add getHealingInterval and updateHeailingInterval query

public interface HealingIntervalOperator
{
    @Insert
    CompletableFuture<HealingIntervalEntity> writeHealingInterval(HealingIntervalEntity healingInterval, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("")
    CompletableFuture<HealingIntervalEntity> getHealingInterval(String healingIntervalId, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("")
    CompletableFuture<HealingIntervalEntity> updateHeailingInterval(String healingIntervalId,
                                                                    int handledEventsNumber,
                                                                    Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
