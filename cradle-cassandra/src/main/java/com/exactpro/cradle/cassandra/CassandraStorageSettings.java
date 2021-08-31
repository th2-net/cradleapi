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
import com.exactpro.cradle.cassandra.connection.NetworkTopologyStrategy;

public class CassandraStorageSettings
{
	public static final String CRADLE_INFO_KEYSPACE = "cradle_info",
			BOOKS_TABLE = "books",
			PAGES_TABLE = "pages",
			MESSAGES_TABLE = "messages",
			SESSIONS_TABLE = "sessions",
			SESSIONS_DATES_TABLE = "sessions_dates",
			TEST_EVENTS_TABLE = "test_events",
			SCOPES_TABLE = "scopes",
			TEST_EVENTS_DATES_TABLE = "test_events_dates",
			TEST_EVENT_PARENT_INDEX = "test_event_parent_index",
			LABELS_TABLE = "labels",
			INTERVALS_TABLE = "intervals";
	public static final long DEFAULT_TIMEOUT = 5000;
	public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
	public static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1,
			DEFAULT_MAX_PARALLEL_QUERIES = 500,
			DEFAULT_RESULT_PAGE_SIZE = 0,  //Driver default will be used in this case.
			DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE = 5*1024,
			DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE = 5*1024,
			DEFAULT_MESSAGE_BATCH_CHUNK_SIZE = 1024*1024,
			DEFAULT_TEST_EVENT_CHUNK_SIZE = 1024*1024,
			DEFAULT_TEST_EVENT_MESSAGES_PER_CHUNK = 10;
	
	
	private final NetworkTopologyStrategy networkTopologyStrategy;
	private final long timeout;
	private final ConsistencyLevel writeConsistencyLevel,
			readConsistencyLevel;
	private String cradleInfoKeyspace,
			booksTable,
			pagesTable,
			messagesTable,
			sessionsTable,
			sessionsDatesTable,
			testEventsTable,
			scopesTable,
			testEventsDatesTable,
			testEventParentIndex,
			labelsTable,
			intervalsTable;
  private int keyspaceReplicationFactor;
	
	private int maxParallelQueries,
			resultPageSize,
			maxUncompressedMessageBatchSize,
			maxUncompressedTestEventSize,
			messageBatchChunkSize,
			testEventChunkSize,
			testEventMessagesPerChunk;
	
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
		this.booksTable = BOOKS_TABLE;
		this.pagesTable = PAGES_TABLE;
		this.messagesTable = MESSAGES_TABLE;
		this.sessionsTable = SESSIONS_TABLE;
		this.sessionsDatesTable = SESSIONS_DATES_TABLE;
		this.testEventsTable = TEST_EVENTS_TABLE;
		this.scopesTable = SCOPES_TABLE;
		this.testEventsDatesTable = TEST_EVENTS_DATES_TABLE;
		this.testEventParentIndex = TEST_EVENT_PARENT_INDEX;
		this.labelsTable = LABELS_TABLE;
		this.intervalsTable = INTERVALS_TABLE;
		
		this.keyspaceReplicationFactor = DEFAULT_KEYSPACE_REPL_FACTOR;
		this.maxParallelQueries = DEFAULT_MAX_PARALLEL_QUERIES;
		this.resultPageSize = DEFAULT_RESULT_PAGE_SIZE;
		this.maxUncompressedMessageBatchSize = DEFAULT_MAX_UNCOMPRESSED_MESSAGE_BATCH_SIZE;
		this.maxUncompressedTestEventSize = DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE;
		this.messageBatchChunkSize = DEFAULT_MESSAGE_BATCH_CHUNK_SIZE;
		this.testEventChunkSize = DEFAULT_TEST_EVENT_CHUNK_SIZE;
		this.testEventMessagesPerChunk = DEFAULT_TEST_EVENT_MESSAGES_PER_CHUNK;
	}
	
	public CassandraStorageSettings(CassandraStorageSettings settings)
	{
		this.networkTopologyStrategy = settings.getNetworkTopologyStrategy();
		this.timeout = settings.getTimeout();
		this.writeConsistencyLevel = settings.getWriteConsistencyLevel();
		this.readConsistencyLevel = settings.getReadConsistencyLevel();
		
		this.cradleInfoKeyspace = settings.getCradleInfoKeyspace();
		this.booksTable = settings.getBooksTable();
		this.pagesTable = settings.getPagesTable();
		this.messagesTable = settings.getMessagesTable();
		this.sessionsTable = settings.getSessionsTable();
		this.sessionsDatesTable = settings.getSessionsDatesTable();
		this.testEventsTable = settings.getTestEventsTable();
		this.scopesTable = settings.getScopesTable();
		this.testEventsDatesTable = settings.getTestEventsDatesTable();
		this.testEventParentIndex = settings.getTestEventParentIndex();
		this.labelsTable = settings.getLabelsTable();
		this.intervalsTable = settings.getIntervalsTable();
		
		this.keyspaceReplicationFactor = settings.getKeyspaceReplicationFactor();
		this.maxParallelQueries = settings.getMaxParallelQueries();
		this.resultPageSize = settings.getResultPageSize();
		this.maxUncompressedMessageBatchSize = settings.getMaxUncompressedMessageBatchSize();
		this.maxUncompressedTestEventSize = settings.getMaxUncompressedTestEventSize();
		this.messageBatchChunkSize = settings.getMessageBatchChunkSize();
		this.testEventChunkSize = settings.getTestEventChunkSize();
		this.testEventMessagesPerChunk = settings.getTestEventMessagesPerChunk();
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
	
	
	public String getBooksTable()
	{
		return booksTable;
	}
	
	public void setBooksTable(String booksTable)
	{
		this.booksTable = booksTable;
	}
	
	
	public String getPagesTable()
	{
		return pagesTable;
	}
	
	public void setPagesTable(String pagesTable)
	{
		this.pagesTable = pagesTable;
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
	
	
	public String getSessionsDatesTable()
	{
		return sessionsDatesTable;
	}
	
	public void setSessionsDatesTable(String sessionsDatesTable)
	{
		this.sessionsDatesTable = sessionsDatesTable;
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
	
	
	public String getTestEventsDatesTable()
	{
		return testEventsDatesTable;
	}
	
	public void setTestEventsDatesTable(String testEventsDatesTable)
	{
		this.testEventsDatesTable = testEventsDatesTable;
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
	
	
	public int getMaxUncompressedMessageBatchSize()
	{
		return maxUncompressedMessageBatchSize;
	}
	
	public void setMaxUncompressedMessageBatchSize(int maxUncompressedMessageBatchSize)
	{
		this.maxUncompressedMessageBatchSize = maxUncompressedMessageBatchSize;
	}
	
	
	public int getMaxUncompressedTestEventSize()
	{
		return maxUncompressedTestEventSize;
	}
	
	public void setMaxUncompressedTestEventSize(int maxUncompressedTestEventSize)
	{
		this.maxUncompressedTestEventSize = maxUncompressedTestEventSize;
	}
	
	
	public int getMessageBatchChunkSize()
	{
		return messageBatchChunkSize;
	}
	
	public void setMessageBatchChunkSize(int messageBatchChunkSize)
	{
		this.messageBatchChunkSize = messageBatchChunkSize;
	}
	
	
	public int getTestEventChunkSize()
	{
		return testEventChunkSize;
	}
	
	public void setTestEventChunkSize(int testEventChunkSize)
	{
		this.testEventChunkSize = testEventChunkSize;
	}
	
	
	public int getTestEventMessagesPerChunk()
	{
		return testEventMessagesPerChunk;
	}
	
	public void setTestEventMessagesPerChunk(int testEventMessagesPerChunk)
	{
		this.testEventMessagesPerChunk = testEventMessagesPerChunk;
	}
}