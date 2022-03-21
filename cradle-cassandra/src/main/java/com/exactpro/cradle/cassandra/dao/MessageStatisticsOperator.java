package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Update;

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
            FRAME_START + "<=:frameEnd")
    CompletableFuture<MappedAsyncPagingIterable<MessageStatisticsEntity>> getStatistics (
            String sessionAlias,
            String direction,
            Byte frameType,
            Instant frameStart,
            Instant frameEnd,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );

    @Update
    @Query("UPDATE ${qualifiedTableId}  SET entity_count = entity_count + :count, entity_size = entity_size + :size WHERE " +
            ENTITY_TYPE + "=:entityType AND " +
            FRAME_TYPE + "=:frameType AND " +
            FRAME_START + "=:frameStart")
    void updateEntities(
            Byte entityType,
            Byte frameType,
            Instant frameStart,
            long count,
            long size,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );
    @Update
    @Query("UPDATE ${qualifiedTableId}  SET entity_count = entity_count + :count, entity_size = entity_size + :size WHERE " +
            SESSION_ALIAS + "=:sessionAlias AND " +
            DIRECTION + "=:direction AND " +
            ENTITY_TYPE + "=:entityType AND " +
            FRAME_TYPE + "=:frameType AND " +
            FRAME_START + "=:frameStart")
    void updateMessages(
            String sessionAlias,
            String direction,
            Byte frameType,
            Instant frameStart,
            long count,
            long size,
            Function<BoundStatementBuilder, BoundStatementBuilder> attributes
    );

}