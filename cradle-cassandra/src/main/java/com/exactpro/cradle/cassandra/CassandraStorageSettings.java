/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
			SCHEMA_VERSION = "5.0.0",
			BOOKS_TABLE = "books",
			BOOKS_STATUS_TABLE = "books_status",
			MESSAGES_TABLE = "messages",
			GROUPED_MESSAGES_TABLE = "grouped_messages",
			TEST_EVENTS_TABLE = "test_events",
			TEST_EVENT_PARENT_INDEX = "test_event_parent_index";
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
			messagesTable,
			groupedMessagesTable,
			testEventsTable,
			testEventParentIndex;
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
			counterPersistenceInterval;

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
		this.messagesTable = MESSAGES_TABLE;
		this.groupedMessagesTable = GROUPED_MESSAGES_TABLE;
		this.testEventsTable = TEST_EVENTS_TABLE;
		this.testEventParentIndex = TEST_EVENT_PARENT_INDEX;

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
		this.counterPersistenceInterval = DEFAULT_COUNTER_PERSISTANE_INTERVAL_MS;
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
		this.messagesTable = settings.getMessagesTable();
		this.groupedMessagesTable = settings.getGroupedMessagesTable();
		this.testEventsTable = settings.getTestEventsTable();
		this.testEventParentIndex = settings.getTestEventParentIndex();

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
		this.counterPersistenceInterval = settings.getCounterPersistenceInterval();
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


	public String getSchemaVersion()
	{
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

	public String getBooksStatusTable()
	{
		return booksStatusTable;
	}

	public void setBooksStatusTable(String booksStatusTable)
	{
		this.booksStatusTable = booksStatusTable;
	}

	public String getMessagesTable()
	{
		return messagesTable;
	}

	public void setMessagesTable(String messagesTable)
	{
		this.messagesTable = messagesTable;
	}

	public String getTestEventsTable()
	{
		return testEventsTable;
	}

	public void setTestEventsTable(String testEventsTable)
	{
		this.testEventsTable = testEventsTable;
	}

	public String getTestEventParentIndex()
	{
		return testEventParentIndex;
	}

	public void setTestEventParentIndex(String testEventParentIndex)
	{
		this.testEventParentIndex = testEventParentIndex;
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

	public int getCounterPersistenceInterval()
	{
		return counterPersistenceInterval;
	}

	public void setCounterPersistenceInterval(int counterPersistenceInterval)
	{
		this.counterPersistenceInterval = counterPersistenceInterval;
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

	public String getGroupedMessagesTable()
	{
		return groupedMessagesTable;
	}

	public void setGroupedMessagesTable(String groupedMessagesTable)
	{
		this.groupedMessagesTable = groupedMessagesTable;
	}
}
