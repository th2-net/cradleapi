package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxLengthEntity;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxLengthOperator;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class EventBatchLengthCache {

    private Logger logger = LoggerFactory.getLogger(EventBatchLengthCache.class);

    public static class CacheKey {
        private final UUID uuid;
        private final LocalDate date;

        public  CacheKey (UUID uuid, LocalDate date) {
            this.uuid = uuid;
            this.date = date;
        }

        public UUID getUuid() {
            return uuid;
        }

        public LocalDate getDate() {
            return date;
        }
    }

    private final Cache<CacheKey, Long> lengthsCache;
    private final EventBatchMaxLengthOperator operator;
    Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs;
    long defaultBatchLengthMillis;


    public EventBatchLengthCache(
            EventBatchMaxLengthOperator operator,
            Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
            Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
            int limit,
            long defaultBatchLengthMillis) {
        this.operator = operator;
        this.readAttrs = readAttrs;
        this.writeAttrs = writeAttrs;
        this.lengthsCache = CacheBuilder.newBuilder().maximumSize(limit).build();
        this.defaultBatchLengthMillis = defaultBatchLengthMillis;
    }

    public long updateMaxLength (CacheKey key, long length) throws CradleStorageException {
        long cachedLength;
        try {
            cachedLength = lengthsCache.get(key, () -> operator.writeMaxLength(key.getUuid(), key.getDate(), length, writeAttrs).getMaxBatchLength());
        } catch (ExecutionException e) {
            throw new CradleStorageException("Could not update batch length ", e);
        }

        long newMaxLength = length;
        if (cachedLength < length) {
            newMaxLength = operator.updateMaxLength(key.getUuid(), key.getDate(), length, writeAttrs).getMaxBatchLength();
            lengthsCache.put(key, newMaxLength);
        }

        return newMaxLength;
    }

    public long getMaxLength (CacheKey key) {
        EventBatchMaxLengthEntity entity = operator.getMaxLength(key.getUuid(), key.getDate(), readAttrs);

        if (entity == null) {
            logger.debug("Could not get max length for key ({}, {}), returning default value", key.getUuid(), key.getDate());

            return defaultBatchLengthMillis;
        }

        return entity.getMaxBatchLength();
    }
}
