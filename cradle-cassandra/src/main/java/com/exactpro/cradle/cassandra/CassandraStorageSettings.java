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
	public static final String STREAMS_TABLE_DEFAULT_NAME = "streams";
	public static final String INSTANCES_TABLE_DEFAULT_NAME = "instances";
	public static final String MESSAGES_TABLE_DEFAULT_NAME = "messages";
	public static final String REPORTS_TABLE_DEFAULT_NAME = "reports";
	public static final String BATCH_DIRECTION_METADATA_TABLE_DEFAULT_NAME = "batch_dir_metadata";
	public static final String BATCH_STREAMS_METADATA_TABLE_DEFAULT_NAME = "batch_streams_metadata";
	public static final String REPORT_MSGS_LINK_TABLE_DEFAULT_NAME = "report_msgs_link";
	private static final String DEFAULT_KEYSPACE = "cradle";
	private static final long DEFAULT_TIMEOUT = 5000;
	private static final int DEFAULT_KEYSPACE_REPL_FACTOR = 1;
	protected static final int BATCH_MESSAGES_LIMIT = 10;
	protected static final int REPORT_MSGS_LINK_MAX_MSGS = 10;

	private final String keyspace;
	private final String messagesTableName;
	private final String batchDirMetadataTableName;
	private final String batchStreamsMetadataTableName;
	private final String reportsTableName;
	private final String reportMsgsLinkTableName;
	private final int keyspaceReplicationFactor;
	private long timeout;

	public CassandraStorageSettings(String messagesTableName, String batchDirMetadataTableName,
	                                String batchStreamsMetadataTableName, String reportsTableName,
	                                String reportMsgsLinkTableName, String keyspace, int keyspaceReplicationFactor,
	                                long timeout)
	{
		this.messagesTableName = messagesTableName;
		this.batchDirMetadataTableName = batchDirMetadataTableName;
		this.batchStreamsMetadataTableName = batchStreamsMetadataTableName;
		this.reportsTableName = reportsTableName;
		this.reportMsgsLinkTableName = reportMsgsLinkTableName;
		this.keyspace = keyspace;
		this.keyspaceReplicationFactor = keyspaceReplicationFactor;
		this.timeout = timeout;
	}

	public CassandraStorageSettings(String keyspace, int keyspaceReplicationFactor)
	{
		this(MESSAGES_TABLE_DEFAULT_NAME, BATCH_DIRECTION_METADATA_TABLE_DEFAULT_NAME,
				BATCH_STREAMS_METADATA_TABLE_DEFAULT_NAME, REPORTS_TABLE_DEFAULT_NAME, REPORT_MSGS_LINK_TABLE_DEFAULT_NAME,
				keyspace, keyspaceReplicationFactor, DEFAULT_TIMEOUT);
	}

	public CassandraStorageSettings(String keyspace)
	{
		this(MESSAGES_TABLE_DEFAULT_NAME, BATCH_DIRECTION_METADATA_TABLE_DEFAULT_NAME,
				BATCH_STREAMS_METADATA_TABLE_DEFAULT_NAME, REPORTS_TABLE_DEFAULT_NAME, REPORT_MSGS_LINK_TABLE_DEFAULT_NAME,
				keyspace, DEFAULT_KEYSPACE_REPL_FACTOR, DEFAULT_TIMEOUT);
	}

	public CassandraStorageSettings() {
		this(MESSAGES_TABLE_DEFAULT_NAME, BATCH_DIRECTION_METADATA_TABLE_DEFAULT_NAME,
				BATCH_STREAMS_METADATA_TABLE_DEFAULT_NAME, REPORTS_TABLE_DEFAULT_NAME, REPORT_MSGS_LINK_TABLE_DEFAULT_NAME,
				DEFAULT_KEYSPACE, DEFAULT_KEYSPACE_REPL_FACTOR, DEFAULT_TIMEOUT);
	}

	public String getKeyspace()
	{
		return keyspace;
	}

	public String getMessagesTableName()
	{
		return messagesTableName;
	}

	public String getBatchDirMetadataTableName()
	{
		return batchDirMetadataTableName;
	}

	public String getBatchStreamsMetadataTableName()
	{
		return batchStreamsMetadataTableName;
	}

	public String getReportsTableName()
	{
		return reportsTableName;
	}

	public String getReportMsgsLinkTableName()
	{
		return reportMsgsLinkTableName;
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
