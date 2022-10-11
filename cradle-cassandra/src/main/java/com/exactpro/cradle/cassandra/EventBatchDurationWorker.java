package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationEntity;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationOperator;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class EventBatchDurationWorker {

    private final Logger logger = LoggerFactory.getLogger(EventBatchDurationWorker.class);

    private final EventBatchDurationCache cache;
    private final EventBatchMaxDurationOperator operator;
    private final long defaultBatchDurationMillis;



    public EventBatchDurationWorker(EventBatchDurationCache cache, EventBatchMaxDurationOperator operator, long defaultBatchDurationMillis) {
        this.cache = cache;
        this.operator = operator;
        this.defaultBatchDurationMillis = defaultBatchDurationMillis;
    }

    public CompletableFuture<Void> updateMaxDuration(EventBatchDurationCache.CacheKey key, long duration, Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs) throws CradleStorageException {
        Long cachedDuration = cache.getMaxDuration(key);

        if (cachedDuration != null) {
            if (cachedDuration > duration) {
                return CompletableFuture.completedFuture(null);
            }
        }

        return operator.writeMaxDuration(new EventBatchMaxDurationEntity(key.getBook(), key.getPage(), key.getScope(), duration), writeAttrs)
                .thenAcceptAsync((res) -> operator.updateMaxDuration(key.getBook(), key.getPage(), key.getScope(), duration, duration, writeAttrs))
                .whenComplete((rtn, e) -> {
                    if (e == null) {
                        cache.updateCache(key, duration);
                    }
                });
    }

    public void removePageDurations (PageId pageId) {
        List<EventBatchDurationCache.CacheKey> keysToRemove = new ArrayList<>();

        // Remove from cache
        cache.removePageDurations(pageId);

        // Remove from database
        logger.trace("{} EventBatchMaxDurationEntity will be removed from database", keysToRemove.size());
        operator.removeMaxDurations(pageId.getBookId().getName(), pageId.getName());
    }

    public long getMaxDuration(EventBatchDurationCache.CacheKey key, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) {
        EventBatchMaxDurationEntity entity = operator.getMaxDuration(key.getBook(), key.getPage(), key.getScope(), readAttrs);

        if (entity == null) {
            logger.trace("Could not get max duration for key ({}, {}, {}), returning default value",
                    key.getBook(), key.getPage(), key.getScope());

            return defaultBatchDurationMillis;
        }

        return entity.getMaxBatchDuration();
    }
}
