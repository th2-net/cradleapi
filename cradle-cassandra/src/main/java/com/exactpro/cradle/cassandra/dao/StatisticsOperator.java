package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.EntityType;
import com.exactpro.cradle.FrameType;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface StatisticsOperator {

    @Query("SELECT * FROM ${qualifiedTableId}  WHERE " + SESSION_ALIAS + "=:sessionAlias AND " +
            DIRECTION + "=:direction AND " +
            ENTITY_TYPE + "=:entityType AND " +
            FRAME_TYPE + "=:frameType AND " +
            FRAME_START + ">=:frameStart AND " +
            FRAME_START + "<=:frameEnd")
    CompletableFuture<MappedAsyncPagingIterable<StatisticsEntity>> getStatistics (
            String sessionAlias,
            String direction,
            Byte entityType,
            Byte frameType,
            Instant frameStart,
            Instant frameEnd,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );
}
