package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.exactpro.cradle.healing.HealingInterval;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.HEALED_EVENTS_NUMBER;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_MAX_LENGTH;
import static com.exactpro.cradle.cassandra.StorageConstants.HEALING_INTERVAL_START_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.RECOVERY_STATE_ID;

@Dao
public interface HealingIntervalOperator
{
    @Insert
    CompletableFuture<HealingIntervalEntity> writeHealingInterval(HealingIntervalEntity healingIntervalEntity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+HEALING_INTERVAL_ID+"=:healingIntervalId")
    CompletableFuture<HealingIntervalEntity> getHealingInterval(UUID instanceId,
                                                                String healingIntervalId,
                                                                Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

    //FIXME: I need to have params with all fields on healingInterval, not just instance of that class
    @Query("UPDATE ${qualifiedTableId} SET "+HEALING_INTERVAL_START_TIME+"=:healingIntervalStartTime, " +
            ""+HEALING_INTERVAL_MAX_LENGTH+"=:healingIntervalMaxLength, " +
            ""+RECOVERY_STATE_ID+"=:recoveryStateId, " +
            ""+HEALED_EVENTS_NUMBER+"=:healedEventsNumber WHERE " +
            ""+INSTANCE_ID+"=:instanceId AND "+HEALING_INTERVAL_ID+"=:healingIntervalId")
    CompletableFuture<HealingIntervalEntity> updateHeailingInterval(UUID instanceId,
                                                                    Instant startTime,
                                                                    long maxLength,
                                                                    String recoveryStateId,
                                                                    long healedEventsNumber,
                                                                    Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
