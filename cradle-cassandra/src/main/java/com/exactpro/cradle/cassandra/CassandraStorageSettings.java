/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.connection.NetworkTopologyStrategy;
import com.exactpro.cradle.cassandra.retries.SelectExecutionPolicy;

public class CassandraStorageSettings
{
	public static final String CRADLE_INFO_KEYSPACE = "cradle_info",
			SCHEMA_VERSION = "4.3.0",
			BOOKS_TABLE = "books",
			BOOKS_STATUS_TABLE = "books_status",
			PAGES_TABLE = "pages",
			PAGES_NAMES_TABLE = "pages_names",
			MESSAGES_TABLE = "messages",
			SESSIONS_TABLE = "sessions",
			PAGE_SESSIONS_TABLE = "page_sessions",
			TEST_EVENTS_TABLE = "test_events",
			SCOPES_TABLE = "scopes",
			PAGE_SCOPES_TABLE = "page_scopes",
			TEST_EVENT_PARENT_INDEX = "test_event_parent_index",
			LABELS_TABLE = "labels",
			INTERVALS_TABLE = "intervals",
			MESSAGE_STATISTICS_TABLE = "message_statistics",
			ENTITY_STATISTICS_TABLE = "entity_statistics",
			SESSION_STATISTICS_TABLE = "session_statistics";
	public static final long DEFAULT_TIMEOUT = 5000;
	public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
	public static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1,
			DEFAULT_MAX_PARALLEL_QUERIES = 500,
			DEFAULT_RESULT_PAGE_SIZE = 0,  //Driver default will be used in this case.
			DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE = 5 * 1024,
			DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE = 5 * 1024,
			DEFAULT_SESSIONS_CACHE_SIZE = 100,
			DEFAULT_SCOPES_CACHE_SIZE = 10,
			DEFAULT_PAGE_SESSION_CACHE_SIZE = 100,
			DEFAULT_PAGE_SCOPES_CACHE_SIZE = 100,
			DEFAULT_COUNTER_PERSISTANE_INTERVAL_MS = 1000;

	private final NetworkTopologyStrategy networkTopologyStrategy;
	private final long timeout;
	private final ConsistencyLevel writeConsistencyLevel,
			readConsistencyLevel;
	private String cradleInfoKeyspace,
			schemaVersion,
			booksTable,
			booksStatusTable,
			pagesTable,
			pagesNamesTable,
			messagesTable,
			sessionsTable,
			pageSessionsTable,
			testEventsTable,
			scopesTable,
			pageScopesTable,
			testEventParentIndex,
			labelsTable,
			intervalsTable,
			messageStatisticsTable,
			entityStatisticsTable,
			sessionStatisticsTable;
	private int keyspaceReplicationFactor;
	
	private int maxParallelQueries,
			resultPageSize,
			maxMessageBatchSize,
			maxUncompressedMessageBatchSize,
			maxTestEventBatchSize,
			maxUncompressedTestEventSize,
			
			sessionsCacheSize,
			scopesCacheSize,
			pageSessionsCacheSize,
			pageScopesCacheSize,
			counterPersistanceInterval;

	private SelectExecutionPolicy multiRowResultExecutionPolicy, singleRowResultExecutionPolicy;
	
	public CassandraStorageSettings()
	{
		this(null, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}

	public CassandraStorageSettings(long timeout,
			ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
	{
		this(null, timeout, writeConsistencyLevel, readConsistencyLevel);
	}

	public CassandraStorageSettings(NetworkTopologyStrategy networkTopologyStrategy, long timeout,
			ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
	{
		this.networkTopologyStrategy = networkTopologyStrategy;
		this.timeout = timeout;
		this.writeConsistencyLevel = writeConsistencyLevel;
		this.readConsistencyLevel = readConsistencyLevel;

		this.cradleInfoKeyspace = CRADLE_INFO_KEYSPACE;
		this.schemaVersion = SCHEMA_VERSION;
		this.booksTable = BOOKS_TABLE;
		this.booksStatusTable = BOOKS_STATUS_TABLE;
		this.pagesTable = PAGES_TABLE;
		this.pagesNamesTable = PAGES_NAMES_TABLE;
		this.messagesTable = MESSAGES_TABLE;
		this.sessionsTable = SESSIONS_TABLE;
		this.pageSessionsTable = PAGE_SESSIONS_TABLE;
		this.testEventsTable = TEST_EVENTS_TABLE;
		this.scopesTable = SCOPES_TABLE;
		this.pageScopesTable = PAGE_SCOPES_TABLE;
		this.testEventParentIndex = TEST_EVENT_PARENT_INDEX;
		this.labelsTable = LABELS_TABLE;
		this.intervalsTable = INTERVALS_TABLE;
		this.messageStatisticsTable = MESSAGE_STATISTICS_TABLE;
		this.entityStatisticsTable = ENTITY_STATISTICS_TABLE;
		this.sessionStatisticsTable = SESSION_STATISTICS_TABLE;

		this.keyspaceReplicationFactor = DEFAULT_KEYSPACE_REPL_FACTOR;
		this.maxParallelQueries = DEFAULT_MAX_PARALLEL_QUERIES;
		this.resultPageSize = DEFAULT_RESULT_PAGE_SIZE;
		this.maxMessageBatchSize = CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;
		this.maxUncompressedMessageBatchSize = DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE;
		this.maxTestEventBatchSize = CradleStorage.DEFAULT_MAX_TEST_EVENT_BATCH_SIZE;
		this.maxUncompressedTestEventSize = DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE;
		this.sessionsCacheSize = DEFAULT_SESSIONS_CACHE_SIZE;
		this.pageSessionsCacheSize = DEFAULT_PAGE_SESSION_CACHE_SIZE;
		this.scopesCacheSize = DEFAULT_SCOPES_CACHE_SIZE;
		this.pageScopesCacheSize = DEFAULT_PAGE_SCOPES_CACHE_SIZE;
		this.counterPersistanceInterval = DEFAULT_COUNTER_PERSISTANE_INTERVAL_MS;
	}

	public CassandraStorageSettings(CassandraStorageSettings settings)
	{
		this.networkTopologyStrategy = settings.getNetworkTopologyStrategy();
		this.timeout = settings.getTimeout();
		this.writeConsistencyLevel = settings.getWriteConsistencyLevel();
		this.readConsistencyLevel = settings.getReadConsistencyLevel();
		
		this.cradleInfoKeyspace = settings.getCradleInfoKeyspace();
		this.schemaVersion = settings.getSchemaVersion();
		this.booksTable = settings.getBooksTable();
		this.booksStatusTable = settings.getBooksStatusTable();
		this.pagesTable = settings.getPagesTable();
		this.pagesNamesTable = settings.getPagesNamesTable();
		this.messagesTable = settings.getMessagesTable();
		this.sessionsTable = settings.getSessionsTable();
		this.pageSessionsTable = settings.getPageSessionsTable();
		this.testEventsTable = settings.getTestEventsTable();
		this.scopesTable = settings.getScopesTable();
		this.pageScopesTable = settings.getPageScopesTable();
		this.testEventParentIndex = settings.getTestEventParentIndex();
		this.labelsTable = settings.getLabelsTable();
		this.intervalsTable = settings.getIntervalsTable();
		this.messageStatisticsTable = settings.getMessageStatisticsTable();
		this.entityStatisticsTable = settings.getEntityStatisticsTable();
		this.sessionStatisticsTable = settings.getSessionStatisticsTable();

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
		this.counterPersistanceInterval = settings.getCounterPersistanceInterval();
	}
	
	
	public NetworkTopologyStrategy getNetworkTopologyStrategy()
	{
		return networkTopologyStrategy;
	}

	public long getTimeout()
	{
		return timeout;
	}

	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return writeConsistencyLevel;
	}

	public ConsistencyLevel getReadConsistencyLevel()
	{
		return readConsistencyLevel;
	}


	public String getCradleInfoKeyspace()
	{
		return cradleInfoKeyspace;
	}

	public void setCradleInfoKeyspace(String cradleInfoKeyspace)
	{
		this.cradleInfoKeyspace = cradleInfoKeyspace;
	}


	public String getSchemaVersion() {
		return schemaVersion;
	}


	public String getBooksTable()
	{
		return booksTable;
	}

	public void setBooksTable(String booksTable)
	{
		this.booksTable = booksTable;
	}

	public String getBooksStatusTable () {
		return booksStatusTable;
	}

	public void setBooksStatusTable (String booksStatusTable) {
		this.booksStatusTable = booksStatusTable;
	}

	public String getPagesTable()
	{
		return pagesTable;
	}

	public void setPagesTable(String pagesTable)
	{
		this.pagesTable = pagesTable;
	}


	public String getPagesNamesTable()
	{
		return pagesNamesTable;
	}
	
	public void setPagesNamesTable(String pagesNamesTable)
	{
		this.pagesNamesTable = pagesNamesTable;
	}
	
	
	public String getMessagesTable()
	{
		return messagesTable;
	}

	public void setMessagesTable(String messagesTable)
	{
		this.messagesTable = messagesTable;
	}


	public String getSessionsTable()
	{
		return sessionsTable;
	}

	public void setSessionsTable(String sessionsTable)
	{
		this.sessionsTable = sessionsTable;
	}


	public String getPageSessionsTable()
	{
		return pageSessionsTable;
	}

	public void setPageSessionsTable(String pageSessionsTable)
	{
		this.pageSessionsTable = pageSessionsTable;
	}


	public String getTestEventsTable()
	{
		return testEventsTable;
	}

	public void setTestEventsTable(String testEventsTable)
	{
		this.testEventsTable = testEventsTable;
	}


	public String getScopesTable()
	{
		return scopesTable;
	}

	public void setScopesTable(String scopesTable)
	{
		this.scopesTable = scopesTable;
	}


	public String getPageScopesTable()
	{
		return pageScopesTable;
	}

	public void setPageScopesTable(String pageScopesTable)
	{
		this.pageScopesTable = pageScopesTable;
	}


	public String getTestEventParentIndex()
	{
		return testEventParentIndex;
	}

	public void setTestEventParentIndex(String testEventParentIndex)
	{
		this.testEventParentIndex = testEventParentIndex;
	}


	public String getLabelsTable()
	{
		return labelsTable;
	}

	public void setLabelsTable(String labelsTable)
	{
		this.labelsTable = labelsTable;
	}


	public String getIntervalsTable()
	{
		return intervalsTable;
	}

	public void setIntervalsTable(String intervalsTable)
	{
		this.intervalsTable = intervalsTable;
	}


	public String getMessageStatisticsTable() {
		return messageStatisticsTable;
	}

	public void setMessageStatisticsTable(String messageStatisticsTable) {
		this.messageStatisticsTable = messageStatisticsTable;
	}

	public String getEntityStatisticsTable() {
		return entityStatisticsTable;
	}

	public void setEntityStatisticsTable(String entityStatisticsTable) {
		this.entityStatisticsTable = entityStatisticsTable;
	}

	public String getSessionStatisticsTable () {
		return sessionStatisticsTable;
	}

	public void setSessionStatisticsTable (String sessionStatisticsTable) {
		this.sessionStatisticsTable  = sessionStatisticsTable;
	}

	public int getKeyspaceReplicationFactor()
	{
		return keyspaceReplicationFactor;
	}

	public void setKeyspaceReplicationFactor(int keyspaceReplicationFactor)
	{
		this.keyspaceReplicationFactor = keyspaceReplicationFactor;
	}


	public int getMaxParallelQueries()
	{
		return maxParallelQueries;
	}

	public void setMaxParallelQueries(int maxParallelQueries)
	{
		this.maxParallelQueries = maxParallelQueries;
	}


	public int getResultPageSize()
	{
		return resultPageSize;
	}

	public void setResultPageSize(int resultPageSize)
	{
		this.resultPageSize = resultPageSize;
	}
	
	
	public int getMaxMessageBatchSize()
	{
		return maxMessageBatchSize;
	}
	
	public void setMaxMessageBatchSize(int maxMessageBatchSize)
	{
		this.maxMessageBatchSize = maxMessageBatchSize;
	}
	
	
	public int getMaxUncompressedMessageBatchSize()
	{
		return maxUncompressedMessageBatchSize;
	}
	
	public void setMaxUncompressedMessageBatchSize(int maxUncompressedMessageBatchSize)
	{
		this.maxUncompressedMessageBatchSize = maxUncompressedMessageBatchSize;
	}
	
	
	public int getMaxTestEventBatchSize()
	{
		return maxTestEventBatchSize;
	}
	
	public void setMaxTestEventBatchSize(int maxTestEventBatchSize)
	{
		this.maxTestEventBatchSize = maxTestEventBatchSize;
	}
	
	
	public int getMaxUncompressedTestEventSize()
	{
		return maxUncompressedTestEventSize;
	}
	
	public void setMaxUncompressedTestEventSize(int maxUncompressedTestEventSize)
	{
		this.maxUncompressedTestEventSize = maxUncompressedTestEventSize;
	}
	
	
	public int getSessionsCacheSize()
	{
		return sessionsCacheSize;
	}
	
	public void setSessionsCacheSize(int sessionsCacheSize)
	{
		this.sessionsCacheSize = sessionsCacheSize;
	}
	
	
	public int getPageSessionsCacheSize()
	{
		return pageSessionsCacheSize;
	}

	public void setPageSessionsCacheSize(int pageSessionsCacheSize)
	{
		this.pageSessionsCacheSize = pageSessionsCacheSize;
	}
	
	
	public int getScopesCacheSize()
	{
		return scopesCacheSize;
	}

	public void setScopesCacheSize(int scopesCacheSize)
	{
		this.scopesCacheSize = scopesCacheSize;
	}


	public int getPageScopesCacheSize()
	{
		return pageScopesCacheSize;
	}

	public void setPageScopesCacheSize(int pageScopesCacheSize)
	{
		this.pageScopesCacheSize = pageScopesCacheSize;
	}

	public int getCounterPersistanceInterval() {
		return counterPersistanceInterval;
	}

	public void setCounterPersistanceInterval(int counterPersistanceInterval) {
		this.counterPersistanceInterval = counterPersistanceInterval;
	}

	public SelectExecutionPolicy getMultiRowResultExecutionPolicy()
	{
		return multiRowResultExecutionPolicy;
	}

	public void setMultiRowResultExecutionPolicy(
			SelectExecutionPolicy multiRowResultExecutionPolicy)
	{
		this.multiRowResultExecutionPolicy = multiRowResultExecutionPolicy;
	}

	public SelectExecutionPolicy getSingleRowResultExecutionPolicy()
	{
		return singleRowResultExecutionPolicy;
	}

	public void setSingleRowResultExecutionPolicy(
			SelectExecutionPolicy singleRowResultExecutionPolicy)
	{
		this.singleRowResultExecutionPolicy = singleRowResultExecutionPolicy;
	}
}
