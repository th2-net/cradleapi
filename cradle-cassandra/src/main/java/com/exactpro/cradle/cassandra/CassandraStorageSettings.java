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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStorageSettings
{
	private Logger logger = LoggerFactory.getLogger(CassandraStorageSettings.class);

	public static final int BATCH_SIZE_LIMIT_BYTES = 5000;
	public static final long BATCH_IDLE_LIMIT = 100;
	public static final int REPORT_SIZE_LIMIT_BYTES = 5000;
	public static final String DEFAULT_KEYSPACE = "cradle",
			INSTANCES_TABLE_DEFAULT_NAME = "instances",
			STREAMS_TABLE_DEFAULT_NAME = "streams",
			MESSAGES_TABLE_DEFAULT_NAME = "messages",
			REPORTS_TABLE_DEFAULT_NAME = "reports",
			BATCH_DIRECTION_METADATA_TABLE_DEFAULT_NAME = "batch_dir_metadata",
			BATCH_STREAMS_METADATA_TABLE_DEFAULT_NAME = "batch_streams_metadata",
			REPORT_MSGS_LINK_TABLE_DEFAULT_NAME = "report_msgs_link";
	private static final long DEFAULT_TIMEOUT = 5000;
	private static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1;
	protected static final int BATCH_MESSAGES_LIMIT = 10;
	protected static final int REPORT_MSGS_LINK_MAX_MSGS = 10;

	private final String keyspace;
	private String messagesTableName,
			batchDirMetadataTableName,
			batchStreamsMetadataTableName,
			reportsTableName,
			reportMsgsLinkTableName;
	private final int keyspaceReplicationFactor;
	private long timeout;

	public CassandraStorageSettings(String keyspace, int keyspaceReplicationFactor, long timeout)
	{
		this.messagesTableName = MESSAGES_TABLE_DEFAULT_NAME;
		this.batchDirMetadataTableName = BATCH_DIRECTION_METADATA_TABLE_DEFAULT_NAME;
		this.batchStreamsMetadataTableName = BATCH_STREAMS_METADATA_TABLE_DEFAULT_NAME;
		this.reportsTableName = REPORTS_TABLE_DEFAULT_NAME;
		this.reportMsgsLinkTableName = REPORT_MSGS_LINK_TABLE_DEFAULT_NAME;
		this.keyspace = keyspace;
		this.keyspaceReplicationFactor = keyspaceReplicationFactor;
		this.timeout = timeout;
	}

	public CassandraStorageSettings(String keyspace, int keyspaceReplicationFactor)
	{
		this(keyspace, keyspaceReplicationFactor, DEFAULT_TIMEOUT);
	}

	public CassandraStorageSettings(String keyspace)
	{
		this(keyspace, DEFAULT_KEYSPACE_REPL_FACTOR, DEFAULT_TIMEOUT);
	}

	public CassandraStorageSettings()
	{
		this(DEFAULT_KEYSPACE, DEFAULT_KEYSPACE_REPL_FACTOR, DEFAULT_TIMEOUT);
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

	
	public String getBatchDirMetadataTableName()
	{
		return batchDirMetadataTableName;
	}

	public void setBatchDirMetadataTableName(String batchDirMetadataTableName)
	{
		this.batchDirMetadataTableName = batchDirMetadataTableName;
	}
	
	
	public String getBatchStreamsMetadataTableName()
	{
		return batchStreamsMetadataTableName;
	}

	public void setBatchStreamsMetadataTableName(String batchStreamsMetadataTableName)
	{
		this.batchStreamsMetadataTableName = batchStreamsMetadataTableName;
	}

	
	public String getReportsTableName()
	{
		return reportsTableName;
	}
	
	public void setReportsTableName(String reportsTableName)
	{
		this.reportsTableName = reportsTableName;
	}
	
	
	public String getReportMsgsLinkTableName()
	{
		return reportMsgsLinkTableName;
	}
	
	public void setReportMsgsLinkTableName(String reportMsgsLinkTableName)
	{
		this.reportMsgsLinkTableName = reportMsgsLinkTableName;
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
}
