package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface MessageStatisticsOperator {

    @Query("SELECT * FROM ${qualifiedTableId}  WHERE " + SESSION_ALIAS + "=:sessionAlias AND " +
            DIRECTION + "=:direction AND " +
            FRAME_TYPE + "=:frameType AND " +
            FRAME_START + ">=:frameStart AND " +
            FRAME_START + "<:frameEnd")
    CompletableFuture<MappedAsyncPagingIterable<MessageStatisticsEntity>> getStatistics (
            String sessionAlias,
            String direction,
            Byte frameType,
            Instant frameStart,
            Instant frameEnd,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );
}