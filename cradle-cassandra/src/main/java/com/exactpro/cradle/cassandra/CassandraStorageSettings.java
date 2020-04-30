/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;

public class CassandraStorageSettings
{
	public static final int MESSAGE_BATCH_SIZE_LIMIT_BYTES = 5000,
			TEST_EVENT_BATCH_SIZE_LIMIT_BYTES = 5000;
	public static final String DEFAULT_KEYSPACE = "cradle",
			INSTANCES_TABLE_DEFAULT_NAME = "instances",
			STREAMS_TABLE_DEFAULT_NAME = "streams",
			MESSAGES_TABLE_DEFAULT_NAME = "messages",
			TEST_EVENTS_TABLE_DEFAULT_NAME = "test_events",
			STREAM_MSGS_LINK_TABLE_DEFAULT_NAME = "stream_messages_link",
			TEST_EVENTS_PARENTS_LINK_TABLE_DEFAULT_NAME = "test_events_parents_link",
			TEST_EVENT_MSGS_LINK_TABLE_DEFAULT_NAME = "test_event_messages_link";
	public static final long DEFAULT_TIMEOUT = 5000;
	public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
	public static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1;
	public static final int BATCH_MESSAGES_LIMIT = 10,
			TEST_EVENTS_MSGS_LINK_MAX_MSGS = 10;

	private final String keyspace;
	private String messagesTableName,
			testEventsTableName,
			streamMsgsLinkTableName,
			testEventsParentsLinkTableName,
			testEventMsgsLinkTableName;
	private final int keyspaceReplicationFactor;
	private long timeout;
	private ConsistencyLevel writeConsistencyLevel,
			readConsistencyLevel;

	public CassandraStorageSettings(String keyspace, int keyspaceReplicationFactor, 
			long timeout, ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
	{
		this.messagesTableName = MESSAGES_TABLE_DEFAULT_NAME;
		this.testEventsTableName = TEST_EVENTS_TABLE_DEFAULT_NAME;
		this.streamMsgsLinkTableName = STREAM_MSGS_LINK_TABLE_DEFAULT_NAME;
		this.testEventsParentsLinkTableName = TEST_EVENTS_PARENTS_LINK_TABLE_DEFAULT_NAME;
		this.testEventMsgsLinkTableName = TEST_EVENT_MSGS_LINK_TABLE_DEFAULT_NAME;
		this.keyspace = keyspace;
		this.keyspaceReplicationFactor = keyspaceReplicationFactor;
		this.timeout = timeout;
		this.writeConsistencyLevel = writeConsistencyLevel;
		this.readConsistencyLevel = readConsistencyLevel;
	}

	public CassandraStorageSettings(String keyspace, int keyspaceReplicationFactor)
	{
		this(keyspace, keyspaceReplicationFactor, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}

	public CassandraStorageSettings(String keyspace)
	{
		this(keyspace, DEFAULT_KEYSPACE_REPL_FACTOR, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}

	public CassandraStorageSettings()
	{
		this(DEFAULT_KEYSPACE, DEFAULT_KEYSPACE_REPL_FACTOR, DEFAULT_TIMEOUT, DEFAULT_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
	}

	public String getKeyspace()
	{
		return keyspace;
	}

	
	public String getMessagesTableName()
	{
		return messagesTableName;
	}
	
	public void setMessagesTableName(String messagesTableName)
	{
		this.messagesTableName = messagesTableName;
	}

	
	public String getTestEventsTableName()
	{
		return testEventsTableName;
	}
	
	public void setTestEventsTableName(String testEventsTableName)
	{
		this.testEventsTableName = testEventsTableName;
	}
	
	
	public String getStreamMsgsLinkTableName()
	{
		return streamMsgsLinkTableName;
	}
	
	public void setStreamMsgsLinkTableName(String streamMsgsLinkTableName)
	{
		this.streamMsgsLinkTableName = streamMsgsLinkTableName;
	}
	
	
	public String getTestEventsParentsLinkTableName()
	{
		return testEventsParentsLinkTableName;
	}
	
	public void setTestEventsParentsLinkTableName(String testEventsParentsLinkTableName)
	{
		this.testEventsParentsLinkTableName = testEventsParentsLinkTableName;
	}
	
	
	public String getTestEventMsgsLinkTableName()
	{
		return testEventMsgsLinkTableName;
	}
	
	public void setTestEventMsgsLinkTableName(String testEventMsgsLinkTableName)
	{
		this.testEventMsgsLinkTableName = testEventMsgsLinkTableName;
	}
	
	
	public int getKeyspaceReplicationFactor()
	{
		return keyspaceReplicationFactor;
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
}