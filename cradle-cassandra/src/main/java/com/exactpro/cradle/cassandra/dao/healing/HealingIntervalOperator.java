package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.exactpro.cradle.healing.HealingInterval;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.ID;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;

@Dao
public interface HealingIntervalOperator
{
    @Insert
    CompletableFuture<HealingIntervalEntity> writeHealingInterval(HealingIntervalEntity healingIntervalEntity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+ID+"=:id")
    CompletableFuture<HealingIntervalEntity> getHealingInterval(UUID instanceId,
                                                                String healingIntervalId,
                                                                Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    //FIXME: I need to have params with all fields on healingInterval, not just instance of that class
    @Query("UPDATE ${qualifiedTableId} SET "+HEALING_INTERVAL_ID+"=:healing_interval_id WHERE "+INSTANCE_ID+"=:instanceId AND "+ID+"=:id")
    CompletableFuture<HealingIntervalEntity> updateHeailingInterval(UUID instanceId,
                                                                    String healingIntervalId,
                                                                    Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
