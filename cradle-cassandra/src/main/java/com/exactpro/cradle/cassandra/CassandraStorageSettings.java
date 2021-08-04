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
	public static final String PAGES_TABLE = "pages",
			MESSAGES_TABLE = "messages",
			SESSION_TABLE = "sessions",
			TEST_EVENTS_TABLE = "test_events",
			TEST_EVENTS_DATES_TABLE = "test_events_dates",
			LABELS_TABLE = "labels",
			INTERVALS_TABLE = "intervals";
	public static final long DEFAULT_TIMEOUT = 5000;
	public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
	public static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1,
			DEFAULT_MAX_PARALLEL_QUERIES = 500,
			DEFAULT_RESULT_PAGE_SIZE = 0;  //Driver default will be used in this case.
	
	
	private final NetworkTopologyStrategy networkTopologyStrategy;
	private final long timeout;
	private final ConsistencyLevel writeConsistencyLevel,
			readConsistencyLevel;
	private String pagesTable,
			messagesTable,
			sessionsTable,
			testEventsTable,
			testEventsDatesTable,
			labelsTable,
			intervalsTable;
  private int keyspaceReplicationFactor;
	
	private int maxParallelQueries,
			resultPageSize;
	
	public CassandraStorageSettings()
	{
		this(null, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}
	
	public CassandraStorageSettings(NetworkTopologyStrategy networkTopologyStrategy, long timeout, 
			ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
	{
		this.networkTopologyStrategy = networkTopologyStrategy;
		this.timeout = timeout;
		this.writeConsistencyLevel = writeConsistencyLevel;
		this.readConsistencyLevel = readConsistencyLevel;
		
		this.pagesTable = PAGES_TABLE;
		this.messagesTable = MESSAGES_TABLE;
		this.sessionsTable = SESSION_TABLE;
		this.testEventsTable = TEST_EVENTS_TABLE;
		this.testEventsDatesTable = TEST_EVENTS_DATES_TABLE;
		this.labelsTable = LABELS_TABLE;
		this.intervalsTable = INTERVALS_TABLE;
		
		this.keyspaceReplicationFactor = DEFAULT_KEYSPACE_REPL_FACTOR;
		this.maxParallelQueries = DEFAULT_MAX_PARALLEL_QUERIES;
		this.resultPageSize = DEFAULT_RESULT_PAGE_SIZE;
	}
	
	public CassandraStorageSettings(CassandraStorageSettings settings)
	{
		this.networkTopologyStrategy = settings.getNetworkTopologyStrategy();
		this.timeout = settings.getTimeout();
		this.writeConsistencyLevel = settings.getWriteConsistencyLevel();
		this.readConsistencyLevel = settings.getReadConsistencyLevel();
		
		this.pagesTable = settings.getPagesTable();
		this.messagesTable = settings.getMessagesTable();
		this.sessionsTable = settings.getSessionsTable();
		this.testEventsTable = settings.getTestEventsTable();
		this.testEventsDatesTable = settings.getTestEventsDatesTable();
		this.labelsTable = settings.getLabelsTable();
		this.intervalsTable = settings.getIntervalsTable();
		
		this.keyspaceReplicationFactor = settings.getKeyspaceReplicationFactor();
		this.maxParallelQueries = settings.getMaxParallelQueries();
		this.resultPageSize = settings.getResultPageSize();
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
	
	
	public String getTestEventsTable()
	{
		return testEventsTable;
	}
	
	public void setTestEventsTable(String testEventsTable)
	{
		this.testEventsTable = testEventsTable;
	}
	
	
	public String getTestEventsDatesTable()
	{
		return testEventsDatesTable;
	}
	
	public void setTestEventsDatesTable(String testEventsDatesTable)
	{
		this.testEventsDatesTable = testEventsDatesTable;
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
}