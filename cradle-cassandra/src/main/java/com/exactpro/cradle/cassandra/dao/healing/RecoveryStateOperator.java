package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

//TODO: add getRecoveryState query

@Dao
public interface RecoveryStateOperator
{
    @Insert
    CompletableFuture<RecoveryStateEntity> writeRecoveryState(RecoveryStateEntity recoveryState,
                                                              Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

}
