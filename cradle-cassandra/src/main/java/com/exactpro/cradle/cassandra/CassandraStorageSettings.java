/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.cradle.cassandra;

import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.cassandra.connection.NetworkTopologyStrategy;
import com.exactpro.cradle.cassandra.retries.SelectExecutionPolicy;
import com.exactpro.cradle.utils.CompressionType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

import static com.exactpro.cradle.CradleStorage.DEFAULT_COMPOSING_SERVICE_THREADS;
import static com.exactpro.cradle.CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;
import static com.exactpro.cradle.CradleStorage.DEFAULT_MAX_TEST_EVENT_BATCH_SIZE;

@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class CassandraStorageSettings extends CoreStorageSettings {
    public static final String SCHEMA_VERSION = "5.3.0";
    public static final int RANDOM_ACCESS_DAYS_CACHE_SIZE = 10;
    /** One day in milliseconds */
    public static final long RANDOM_ACCESS_DAYS_CACHE_INVALIDATE_INTERVAL = 24 * 60 * 60 * 1_000;

    public static final CassandraConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = CassandraConsistencyLevel.LOCAL_QUORUM;
    public static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1;
    public static final int DEFAULT_MAX_PARALLEL_QUERIES = 500;
    public static final int DEFAULT_RESULT_PAGE_SIZE = 0;
    public static final int DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE = 5 * 1024;
    public static final int DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE = 5 * 1024;
    public static final int DEFAULT_SESSIONS_CACHE_SIZE = 100;
    public static final int DEFAULT_SCOPES_CACHE_SIZE = 10;
    public static final int DEFAULT_PAGE_SESSION_CACHE_SIZE = 100;
    public static final int DEFAULT_PAGE_SCOPES_CACHE_SIZE = 100;
    public static final int DEFAULT_SESSION_STATISTICS_CACHE_SIZE = 10_000;
    public static final int DEFAULT_GROUPS_CACHE_SIZE = 10_000;
    public static final int DEFAULT_EVENT_BATCH_DURATION_CACHE_SIZE = 5_000;
    public static final int DEFAULT_PAGE_GROUPS_CACHE_SIZE = 10_000;
    public static final int DEFAULT_COUNTER_PERSISTENCE_INTERVAL_MS = 15000;
    public static final long DEFAULT_EVENT_BATCH_DURATION_MILLIS = 5_000;
    public static final long DEFAULT_TIMEOUT = 5000;
    public static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.LZ4;

    //we need to use Instant.EPOCH instead of Instant.MIN.
    //when cassandra driver tries to convert Instant.MIN to milliseconds using toEpochMilli() it causes long overflow.
    public static final Instant MIN_EPOCH_INSTANT = Instant.EPOCH;
    //we need to use Instant.ofEpochMilli(Long.MAX_VALUE) instead of Instant.MAX.
    //when cassandra driver tries to convert Instant.MAX to milliseconds using toEpochMilli() it causes long overflow.
    public static final Instant MAX_EPOCH_INSTANT = Instant.ofEpochMilli(Long.MAX_VALUE);
    public static final Instant DEFAULT_PAGE_REMOVE_TIME = MAX_EPOCH_INSTANT;
    public static final long DEFAULT_INIT_OPERATORS_DURATION_SECONDS = 60;

    @JsonIgnore
    private NetworkTopologyStrategy networkTopologyStrategy;
    private long timeout = DEFAULT_TIMEOUT;
    @JsonIgnore
    private CassandraConsistencyLevel writeConsistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
    @JsonIgnore
    private CassandraConsistencyLevel readConsistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
    private String keyspace;
    private String schemaVersion = SCHEMA_VERSION;
    private int randomAccessDaysCacheSize = RANDOM_ACCESS_DAYS_CACHE_SIZE;
    private long randomAccessDaysInvalidateInterval = RANDOM_ACCESS_DAYS_CACHE_INVALIDATE_INTERVAL;
    private int keyspaceReplicationFactor = DEFAULT_KEYSPACE_REPL_FACTOR;

    private int maxParallelQueries = DEFAULT_MAX_PARALLEL_QUERIES; // FIXME: remove
    private int resultPageSize = DEFAULT_RESULT_PAGE_SIZE;
    private int maxMessageBatchSize = DEFAULT_MAX_MESSAGE_BATCH_SIZE;
    private int maxUncompressedMessageBatchSize = DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE;
    private int maxTestEventBatchSize = DEFAULT_MAX_TEST_EVENT_BATCH_SIZE;
    private int maxUncompressedTestEventSize = DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE;
    private int sessionsCacheSize = DEFAULT_SESSIONS_CACHE_SIZE;
    private int scopesCacheSize = DEFAULT_SCOPES_CACHE_SIZE;
    private int pageSessionsCacheSize = DEFAULT_PAGE_SESSION_CACHE_SIZE;
    private int pageScopesCacheSize = DEFAULT_PAGE_SCOPES_CACHE_SIZE;
    private int sessionStatisticsCacheSize = DEFAULT_SESSION_STATISTICS_CACHE_SIZE;
    private int pageGroupsCacheSize = DEFAULT_PAGE_GROUPS_CACHE_SIZE;
    private int groupsCacheSize = DEFAULT_GROUPS_CACHE_SIZE;
    private int eventBatchDurationCacheSize = DEFAULT_EVENT_BATCH_DURATION_CACHE_SIZE;
    private int counterPersistenceInterval = DEFAULT_COUNTER_PERSISTENCE_INTERVAL_MS;
    private int composingServiceThreads = DEFAULT_COMPOSING_SERVICE_THREADS;

    private SelectExecutionPolicy multiRowResultExecutionPolicy;
    private SelectExecutionPolicy singleRowResultExecutionPolicy;

    private long eventBatchDurationMillis = DEFAULT_EVENT_BATCH_DURATION_MILLIS;

    private CompressionType compressionType = DEFAULT_COMPRESSION_TYPE;

    private long initOperatorsDurationSeconds = DEFAULT_INIT_OPERATORS_DURATION_SECONDS;

    public CassandraStorageSettings() {
    }

    public CassandraStorageSettings(
            long timeout,
            CassandraConsistencyLevel writeConsistencyLevel,
            CassandraConsistencyLevel readConsistencyLevel
    ) {
        this(null, timeout, writeConsistencyLevel, readConsistencyLevel);
    }

    public CassandraStorageSettings(
            NetworkTopologyStrategy networkTopologyStrategy, long timeout,
            CassandraConsistencyLevel writeConsistencyLevel, CassandraConsistencyLevel readConsistencyLevel
    ) {
        this();
        this.networkTopologyStrategy = networkTopologyStrategy;
        this.timeout = timeout;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.readConsistencyLevel = readConsistencyLevel;
    }

    public CassandraStorageSettings(CassandraStorageSettings settings) {
        this.networkTopologyStrategy = settings.getNetworkTopologyStrategy();
        this.timeout = settings.getTimeout();
        this.writeConsistencyLevel = settings.getWriteConsistencyLevel();
        this.readConsistencyLevel = settings.getReadConsistencyLevel();

        this.keyspace = settings.getKeyspace();
        this.schemaVersion = settings.getSchemaVersion();
        this.randomAccessDaysCacheSize = settings.getRandomAccessDaysCacheSize();
        this.randomAccessDaysInvalidateInterval = settings.getRandomAccessDaysInvalidateInterval();

        this.keyspaceReplicationFactor = settings.getKeyspaceReplicationFactor();
        this.maxParallelQueries = settings.getMaxParallelQueries();
        this.resultPageSize = settings.getResultPageSize();
        this.maxMessageBatchSize = settings.getMaxMessageBatchSize();
        this.maxUncompressedMessageBatchSize = settings.getMaxUncompressedMessageBatchSize();
        this.maxTestEventBatchSize = settings.getMaxTestEventBatchSize();
        this.maxUncompressedTestEventSize = settings.getMaxUncompressedTestEventSize();
        this.singleRowResultExecutionPolicy = settings.getSingleRowResultExecutionPolicy();
        this.multiRowResultExecutionPolicy = settings.getMultiRowResultExecutionPolicy();

        this.sessionsCacheSize = settings.getSessionsCacheSize();
        this.pageSessionsCacheSize = settings.getPageSessionsCacheSize();
        this.scopesCacheSize = settings.getScopesCacheSize();
        this.pageScopesCacheSize = settings.getPageScopesCacheSize();
        this.pageGroupsCacheSize = settings.getPageGroupsCacheSize();
        this.groupsCacheSize = settings.getGroupsCacheSize();
        this.sessionStatisticsCacheSize = settings.getSessionStatisticsCacheSize();
        this.counterPersistenceInterval = settings.getCounterPersistenceInterval();
        this.composingServiceThreads = settings.getComposingServiceThreads();
        setBookRefreshIntervalMillis(settings.getBookRefreshIntervalMillis());
        this.eventBatchDurationMillis = settings.getEventBatchDurationMillis();
        this.eventBatchDurationCacheSize = settings.getEventBatchDurationCacheSize();

        setStoreIndividualMessageSessions(settings.isStoreIndividualMessageSessions());
        this.compressionType = settings.getCompressionType();

        this.initOperatorsDurationSeconds = settings.getInitOperatorsDurationSeconds();
    }


    public NetworkTopologyStrategy getNetworkTopologyStrategy() {
        return networkTopologyStrategy;
    }

    public void setNetworkTopologyStrategy(NetworkTopologyStrategy networkTopologyStrategy) {
        this.networkTopologyStrategy = networkTopologyStrategy;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public CassandraConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public void setWriteConsistencyLevel(CassandraConsistencyLevel writeConsistencyLevel) {
        this.writeConsistencyLevel = writeConsistencyLevel;
    }

    public CassandraConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public void setReadConsistencyLevel(CassandraConsistencyLevel readConsistencyLevel) {
        this.readConsistencyLevel = readConsistencyLevel;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }


    public String getSchemaVersion() {
        return schemaVersion;
    }

    public int getRandomAccessDaysCacheSize() {
        return randomAccessDaysCacheSize;
    }

    public long getRandomAccessDaysInvalidateInterval() {
        return randomAccessDaysInvalidateInterval;
    }

    public int getKeyspaceReplicationFactor() {
        return keyspaceReplicationFactor;
    }

    public void setKeyspaceReplicationFactor(int keyspaceReplicationFactor) {
        this.keyspaceReplicationFactor = keyspaceReplicationFactor;
    }


    public int getMaxParallelQueries() {
        return maxParallelQueries;
    }

    public void setMaxParallelQueries(int maxParallelQueries) {
        this.maxParallelQueries = maxParallelQueries;
    }


    public int getResultPageSize() {
        return resultPageSize;
    }

    public void setResultPageSize(int resultPageSize) {
        this.resultPageSize = resultPageSize;
    }


    public int getMaxMessageBatchSize() {
        return maxMessageBatchSize;
    }

    public void setMaxMessageBatchSize(int maxMessageBatchSize) {
        this.maxMessageBatchSize = maxMessageBatchSize;
    }


    public int getMaxUncompressedMessageBatchSize() {
        return maxUncompressedMessageBatchSize;
    }

    public void setMaxUncompressedMessageBatchSize(int maxUncompressedMessageBatchSize) {
        this.maxUncompressedMessageBatchSize = maxUncompressedMessageBatchSize;
    }


    public int getMaxTestEventBatchSize() {
        return maxTestEventBatchSize;
    }

    public void setMaxTestEventBatchSize(int maxTestEventBatchSize) {
        this.maxTestEventBatchSize = maxTestEventBatchSize;
    }


    public int getMaxUncompressedTestEventSize() {
        return maxUncompressedTestEventSize;
    }

    public void setMaxUncompressedTestEventSize(int maxUncompressedTestEventSize) {
        this.maxUncompressedTestEventSize = maxUncompressedTestEventSize;
    }


    public int getSessionsCacheSize() {
        return sessionsCacheSize;
    }

    public void setSessionsCacheSize(int sessionsCacheSize) {
        this.sessionsCacheSize = sessionsCacheSize;
    }


    public int getPageSessionsCacheSize() {
        return pageSessionsCacheSize;
    }

    public void setPageSessionsCacheSize(int pageSessionsCacheSize) {
        this.pageSessionsCacheSize = pageSessionsCacheSize;
    }

    public int getSessionStatisticsCacheSize() {
        return sessionStatisticsCacheSize;
    }

    public void setSessionStatisticsCacheSize(int size) {
        this.sessionStatisticsCacheSize = size;
    }

    public int getScopesCacheSize() {
        return scopesCacheSize;
    }

    public void setScopesCacheSize(int scopesCacheSize) {
        this.scopesCacheSize = scopesCacheSize;
    }


    public int getPageScopesCacheSize() {
        return pageScopesCacheSize;
    }

    public void setPageScopesCacheSize(int pageScopesCacheSize) {
        this.pageScopesCacheSize = pageScopesCacheSize;
    }

    public int getPageGroupsCacheSize() {
        return pageGroupsCacheSize;
    }

    public void setPageGroupsCacheSize(int pageGroupsCacheSize) {
        this.pageGroupsCacheSize = pageGroupsCacheSize;
    }

    public int getGroupsCacheSize() {
        return groupsCacheSize;
    }

    public void setGroupsCacheSize(int groupsCacheSize) {
        this.groupsCacheSize = groupsCacheSize;
    }

    public int getCounterPersistenceInterval() {
        return counterPersistenceInterval;
    }

    public void setCounterPersistenceInterval(int counterPersistenceInterval) {
        this.counterPersistenceInterval = counterPersistenceInterval;
    }

    public int getComposingServiceThreads() {
        return composingServiceThreads;
    }

    public void setComposingServiceThreads(int composingServiceThreads) {
        this.composingServiceThreads = composingServiceThreads;
    }

    public SelectExecutionPolicy getMultiRowResultExecutionPolicy() {
        return multiRowResultExecutionPolicy;
    }

    public void setMultiRowResultExecutionPolicy(
            SelectExecutionPolicy multiRowResultExecutionPolicy
    ) {
        this.multiRowResultExecutionPolicy = multiRowResultExecutionPolicy;
    }

    public SelectExecutionPolicy getSingleRowResultExecutionPolicy() {
        return singleRowResultExecutionPolicy;
    }

    public void setSingleRowResultExecutionPolicy(
            SelectExecutionPolicy singleRowResultExecutionPolicy
    ) {
        this.singleRowResultExecutionPolicy = singleRowResultExecutionPolicy;
    }

    public long getEventBatchDurationMillis() {
        return eventBatchDurationMillis;
    }

    public void setEventBatchDurationMillis(long eventBatchDurationMillis) {
        this.eventBatchDurationMillis = eventBatchDurationMillis;
    }

    public int getEventBatchDurationCacheSize() {
        return eventBatchDurationCacheSize;
    }

    public void setEventBatchDurationCacheSize(int eventBatchDurationCacheSize) {
        this.eventBatchDurationCacheSize = eventBatchDurationCacheSize;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public long getInitOperatorsDurationSeconds() {
        return initOperatorsDurationSeconds;
    }

    public void setInitOperatorsDurationSeconds(long initOperatorsDurationSeconds) {
        this.initOperatorsDurationSeconds = initOperatorsDurationSeconds;
    }

    @Override
    public String toString() {
        return "CassandraStorageSettings{" +
                "networkTopologyStrategy=" + networkTopologyStrategy +
                ", timeout=" + timeout +
                ", writeConsistencyLevel=" + writeConsistencyLevel +
                ", readConsistencyLevel=" + readConsistencyLevel +
                ", keyspace='" + keyspace + '\'' +
                ", schemaVersion='" + schemaVersion + '\'' +
                ", randomAccessDaysCacheSize='" + randomAccessDaysCacheSize + '\'' +
                ", randomAccessDaysInvalidateInterval='" + randomAccessDaysInvalidateInterval + '\'' +
                ", keyspaceReplicationFactor=" + keyspaceReplicationFactor +
                ", maxParallelQueries=" + maxParallelQueries +
                ", resultPageSize=" + resultPageSize +
                ", maxMessageBatchSize=" + maxMessageBatchSize +
                ", maxUncompressedMessageBatchSize=" + maxUncompressedMessageBatchSize +
                ", maxTestEventBatchSize=" + maxTestEventBatchSize +
                ", maxUncompressedTestEventSize=" + maxUncompressedTestEventSize +
                ", sessionsCacheSize=" + sessionsCacheSize +
                ", scopesCacheSize=" + scopesCacheSize +
                ", pageSessionsCacheSize=" + pageSessionsCacheSize +
                ", pageScopesCacheSize=" + pageScopesCacheSize +
                ", sessionStatisticsCacheSize=" + sessionStatisticsCacheSize +
                ", pageGroupsCacheSize=" + pageGroupsCacheSize +
                ", groupsCacheSize=" + groupsCacheSize +
                ", eventBatchDurationCacheSize=" + eventBatchDurationCacheSize +
                ", counterPersistenceInterval=" + counterPersistenceInterval +
                ", composingServiceThreads=" + composingServiceThreads +
                ", multiRowResultExecutionPolicy=" + multiRowResultExecutionPolicy +
                ", singleRowResultExecutionPolicy=" + singleRowResultExecutionPolicy +
                ", bookRefreshIntervalMillis=" + getBookRefreshIntervalMillis() +
                ", eventBatchDurationMillis=" + eventBatchDurationMillis +
                ", storeIndividualMessageSessions=" + isStoreIndividualMessageSessions() +
                ", compressionType=" + compressionType +
                ", initOperatorsDurationSeconds=" + initOperatorsDurationSeconds +
                '}';
    }
}
