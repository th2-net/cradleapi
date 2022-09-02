/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;

public class CassandraStorageSettings
{
	public static final int MESSAGE_BATCH_SIZE_LIMIT_BYTES = 5000,
			TEST_EVENT_BATCH_SIZE_LIMIT_BYTES = 5000;
	public static final String DEFAULT_KEYSPACE = "cradle",
			INSTANCES_TABLE_DEFAULT_NAME = "instances",
			MESSAGES_TABLE_DEFAULT_NAME = "messages",
			PROCESSED_MESSAGES_TABLE_DEFAULT_NAME = "processed_messages",
			TIME_MESSAGES_TABLE_DEFAULT_NAME = "messages_timestamps",
			TEST_EVENTS_TABLE_DEFAULT_NAME = "test_events",
			TIME_TEST_EVENTS_TABLE_DEFAULT_NAME = "time_test_events",
			ROOT_TEST_EVENTS_TABLE_DEFAULT_NAME = "root_test_events",
			TEST_EVENTS_CHILDREN_TABLE_DEFAULT_NAME = "test_events_children",
			TEST_EVENTS_CHILDREN_DATES_TABLE_DEFAULT_NAME = "test_events_children_dates",
			INTERVALS_TABLE_DEFAULT_NAME = "intervals",
			EVENT_BATCH_MAX_DURATION_TABLE_DEFAULT_NAME = "event_batch_max_durations";

	public static final long DEFAULT_TIMEOUT = 5000,
			DEFAULT_MAX_MESSAGE_BATCH_SIZE = StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE,
			DEFAULT_MAX_EVENT_BATCH_SIZE = StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE;
	public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
	public static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1;
	public static final int DEFAULT_EVENT_BATCH_DURATION_CACHE_SIZE = 5000;
	public static final long DEFAULT_EVENT_BATCH_DURATION_MILLIS = 5000;

	private final String keyspace;
	private String messagesTableName,
			timeMessagesTableName,
			processedMessagesTableName,
			testEventsTableName,
			timeTestEventsTableName,
			rootTestEventsTableName,
			testEventsChildrenTableName,
			testEventsChildrenDatesTableName,
			timeIntervalsTableName,
			intervalsTableName,
			eventBatchMaxDurationsTableName;
	private final NetworkTopologyStrategy networkTopologyStrategy;
	private long timeout;
	private ConsistencyLevel writeConsistencyLevel,
			readConsistencyLevel;
	private int keyspaceReplicationFactor;
	private long maxMessageBatchSize,
			maxTestEventBatchSize;

	private int eventBatchDurationCacheSize;
	private long eventBatchDurationMillis;

	public CassandraStorageSettings(String keyspace, NetworkTopologyStrategy networkTopologyStrategy,
			long timeout, ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
	{
		this.messagesTableName = MESSAGES_TABLE_DEFAULT_NAME;
		this.processedMessagesTableName = PROCESSED_MESSAGES_TABLE_DEFAULT_NAME;
		this.timeMessagesTableName = TIME_MESSAGES_TABLE_DEFAULT_NAME;
		this.testEventsTableName = TEST_EVENTS_TABLE_DEFAULT_NAME;
		this.timeTestEventsTableName = TIME_TEST_EVENTS_TABLE_DEFAULT_NAME;
		this.rootTestEventsTableName = ROOT_TEST_EVENTS_TABLE_DEFAULT_NAME;
		this.testEventsChildrenTableName = TEST_EVENTS_CHILDREN_TABLE_DEFAULT_NAME;
		this.testEventsChildrenDatesTableName = TEST_EVENTS_CHILDREN_DATES_TABLE_DEFAULT_NAME;
		this.intervalsTableName = INTERVALS_TABLE_DEFAULT_NAME;
		this.eventBatchMaxDurationsTableName = EVENT_BATCH_MAX_DURATION_TABLE_DEFAULT_NAME;
		this.keyspace = keyspace;
		this.networkTopologyStrategy = networkTopologyStrategy;
		this.timeout = timeout;
		this.writeConsistencyLevel = writeConsistencyLevel;
		this.readConsistencyLevel = readConsistencyLevel;
		this.keyspaceReplicationFactor = DEFAULT_KEYSPACE_REPL_FACTOR;
		this.maxMessageBatchSize = DEFAULT_MAX_MESSAGE_BATCH_SIZE;
		this.maxTestEventBatchSize = DEFAULT_MAX_EVENT_BATCH_SIZE;
		this.eventBatchDurationCacheSize = DEFAULT_EVENT_BATCH_DURATION_CACHE_SIZE;
		this.eventBatchDurationMillis = DEFAULT_EVENT_BATCH_DURATION_MILLIS;
	}

	public CassandraStorageSettings(String keyspace, NetworkTopologyStrategy networkTopology)
	{
		this(keyspace, networkTopology, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}

	public CassandraStorageSettings(String keyspace)
	{
		this(keyspace, null, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}

	public CassandraStorageSettings()
	{
		this(DEFAULT_KEYSPACE, null, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}

	public String getKeyspace()
	{
		return keyspace;
	}
	
	public NetworkTopologyStrategy getNetworkTopologyStrategy()
	{
		return networkTopologyStrategy;
	}
	
	
	public String getMessagesTableName()
	{
		return messagesTableName;
	}
	
	public void setMessagesTableName(String messagesTableName)
	{
		this.messagesTableName = messagesTableName;
	}
	
	
	public String getProcessedMessagesTableName()
	{
		return processedMessagesTableName;
	}
	
	public void setProcessedMessagesTableName(String processedMessagesTableName)
	{
		this.processedMessagesTableName = processedMessagesTableName;
	}
	
	
	public String getTimeMessagesTableName()
	{
		return timeMessagesTableName;
	}
	
	public void setTimeMessagesTableName(String timeMessagesTableName)
	{
		this.timeMessagesTableName = timeMessagesTableName;
	}
	
	
	public String getTestEventsTableName()
	{
		return testEventsTableName;
	}
	
	public void setTestEventsTableName(String testEventsTableName)
	{
		this.testEventsTableName = testEventsTableName;
	}
	
	
	public String getTimeTestEventsTableName()
	{
		return timeTestEventsTableName;
	}
	
	public void setTimeTestEventsTableName(String timeTestEventsTableName)
	{
		this.timeTestEventsTableName = timeTestEventsTableName;
	}
	
	
	public String getRootTestEventsTableName()
	{
		return rootTestEventsTableName;
	}
	
	public void setRootTestEventsTableName(String rootTestEventsTableName)
	{
		this.rootTestEventsTableName = rootTestEventsTableName;
	}
	
	
	public String getTestEventsChildrenTableName()
	{
		return testEventsChildrenTableName;
	}
	
	public void setTestEventsChildrenTableName(String testEventsChildrenTableName)
	{
		this.testEventsChildrenTableName = testEventsChildrenTableName;
	}
	
	
	public String getTestEventsChildrenDatesTableName()
	{
		return testEventsChildrenDatesTableName;
	}
	
	public void setTestEventsChildrenDatesTableName(String testEventsChildrenDatesTableName)
	{
		this.testEventsChildrenDatesTableName = testEventsChildrenDatesTableName;
	}
	
	
	public String getIntervalsTableName() { return intervalsTableName; }

	public String getTimeIntervalsTableName() { return timeIntervalsTableName; }

	public void setIntervalsTableName(String intervalsTableName)
	{
		this.intervalsTableName = intervalsTableName;
	}
	
	public int getKeyspaceReplicationFactor()
	{
		return keyspaceReplicationFactor;
	}
	
	public void setKeyspaceReplicationFactor(int keyspaceReplicationFactor)
	{
		this.keyspaceReplicationFactor = keyspaceReplicationFactor;
	}

	public long getTimeout()
	{
		return timeout;
	}
	
	public void setTimeout(long timeout)
	{
		this.timeout = timeout;
	}
	
	
	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return writeConsistencyLevel;
	}
	
	public void setWriteConsistencyLevel(ConsistencyLevel writeConsistencyLevel)
	{
		this.writeConsistencyLevel = writeConsistencyLevel;
	}
	
	
	public ConsistencyLevel getReadConsistencyLevel()
	{
		return readConsistencyLevel;
	}
	
	public void setReadConsistencyLevel(ConsistencyLevel readConsistencyLevel)
	{
		this.readConsistencyLevel = readConsistencyLevel;
	}
	
	
	public long getMaxMessageBatchSize()
	{
		return maxMessageBatchSize;
	}
	
	public void setMaxMessageBatchSize(long maxMessageBatchSize)
	{
		this.maxMessageBatchSize = maxMessageBatchSize;
	}
	
	
	public long getMaxTestEventBatchSize()
	{
		return maxTestEventBatchSize;
	}
	
	public void setMaxTestEventBatchSize(long maxTestEventBatchSize)
	{
		this.maxTestEventBatchSize = maxTestEventBatchSize;
	}

	public int getEventBatchDurationCacheSize() {
		return eventBatchDurationCacheSize;
	}

	public void setEventBatchDurationCacheSize(int eventBatchDurationCacheSize) {
		this.eventBatchDurationCacheSize = eventBatchDurationCacheSize;
	}

	public long getEventBatchDurationMillis() {
		return eventBatchDurationMillis;
	}

	public void setEventBatchDurationMillis(long eventBatchDurationMillis) {
		this.eventBatchDurationMillis = eventBatchDurationMillis;
	}

	public String getEventBatchMaxDurationsTableName() {
		return eventBatchMaxDurationsTableName;
	}

	public void setEventBatchMaxDurationsTableName(String eventBatchMaxDurationsTableName) {
		this.eventBatchMaxDurationsTableName = eventBatchMaxDurationsTableName;
	}
}
