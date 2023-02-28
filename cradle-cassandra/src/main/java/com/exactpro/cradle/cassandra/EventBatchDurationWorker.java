package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationEntity;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationOperator;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void updateMaxDuration(PageId pageId, String scope, long duration, Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs) throws CradleStorageException {
        String book = pageId.getBookId().getName();
        String page = pageId.getName();
        EventBatchMaxDurationEntity entity = new EventBatchMaxDurationEntity(book, page, scope, duration);
        Long cachedDuration = cache.getMaxDuration(book, page, scope);


        if (cachedDuration != null) {
            if (cachedDuration < duration) {
                // we have already persisted some duration before as we have some value in cache,
                // so we can just update the record in the database
                operator.updateMaxDuration(entity, duration, writeAttrs);
                cache.updateCache(book, page, scope, duration);
            }
        } else {
            // we don't have any duration cached, so we don't know if record exists in database
            // first try to insert and if no success then try to update
            boolean inserted = operator.writeMaxDuration(entity, writeAttrs);
            if (!inserted) {
                operator.updateMaxDuration(entity, duration, writeAttrs);
            }
            cache.updateCache(book, page, scope, duration);
        }
    }

    public void removePageDurations (PageId pageId) {
        // Remove from cache
        int invalidatedCnt = cache.removePageDurations(pageId);

        // Remove from database
        logger.trace("{} EventBatchMaxDurationEntity will be removed from database", invalidatedCnt);
        operator.removeMaxDurations(pageId.getBookId().getName(), pageId.getName());
    }

    public long getMaxDuration(String book, String page, String scope, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) {
        EventBatchMaxDurationEntity entity = operator.getMaxDuration(book, page, scope, readAttrs);

        if (entity == null) {
            logger.trace("Could not get max duration for key ({}, {}, {}), returning default value",
                    book, page, scope);

            return defaultBatchDurationMillis;
        }

        return entity.getMaxBatchDuration();
    }
}
