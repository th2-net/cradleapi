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

import java.io.IOException;
import java.util.Optional;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class TablesCreator
{
	private final QueryExecutor exec;;
	private final CassandraStorageSettings settings;
	
	public TablesCreator(QueryExecutor exec, CassandraStorageSettings settings)
	{
		this.exec = exec;
		this.settings = settings;
	}
	
	public void createAll() throws IOException
	{
		createKeyspace();
		createInstancesTable();
		createStreamsTable();
		createMessagesTable();
		createBatchDirMetadataTable();
		createBatchStreamsMetadataTable();
		
		createReportsTable();
		createReportMessagesLinkTable();
		
		createTestEventsTable();
		createTestEventMessagesLinkTable();
	}
	
	public void createKeyspace()
	{
		Optional<KeyspaceMetadata> keyspaceExist = exec.getSession().getMetadata().getKeyspace(settings.getKeyspace());
		if(!keyspaceExist.isPresent())
		{
			CreateKeyspace createKs =
					SchemaBuilder.createKeyspace(settings.getKeyspace()).withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			exec.getSession().execute(createKs.build());
		}
	}

	public void createInstancesTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), CassandraStorageSettings.INSTANCES_TABLE_DEFAULT_NAME).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withColumn(NAME, DataTypes.TEXT);
		
		exec.executeQuery(create.asCql());
	}
	
	public void createStreamsTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), CassandraStorageSettings.STREAMS_TABLE_DEFAULT_NAME).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(STREAM_DATA, DataTypes.TEXT);
		
		exec.executeQuery(create.asCql());
	}

	public void createMessagesTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getMessagesTableName()).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withColumn(TIMESTAMP, DataTypes.TIMESTAMP)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(PREV_ID, DataTypes.UUID);
		
		exec.executeQuery(create.asCql());
	}

	public void createBatchDirMetadataTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getBatchDirMetadataTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(BATCH_ID, DataTypes.UUID)
				.withColumn(DIRECTION, DataTypes.TEXT);
		
		exec.executeQuery(create.asCql());
	}

	public void createBatchStreamsMetadataTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(),
				settings.getBatchStreamsMetadataTableName()).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withColumn(BATCH_ID, DataTypes.UUID)
				.withColumn(STREAM_ID, DataTypes.UUID);
		
		exec.executeQuery(create.asCql());
	}
	
	
	public void createReportsTable() throws IOException
	{
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getReportsTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withClusteringColumn(ID, DataTypes.TIMEUUID)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TIMESTAMP, DataTypes.TIMESTAMP)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql());
	}
	
	public void createReportMessagesLinkTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getReportMsgsLinkTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withClusteringColumn(REPORT_ID, DataTypes.TIMEUUID)
				.withClusteringColumn(MESSAGES_IDS, DataTypes.frozenListOf(DataTypes.TEXT));
		
		exec.executeQuery(create.asCql());
	}
	
	
	public void createTestEventsTable() throws IOException
	{
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTestEventsTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(REPORT_ID, DataTypes.TIMEUUID)
				.withClusteringColumn(ID, DataTypes.TIMEUUID)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(START_TIMESTAMP, DataTypes.TIMESTAMP)
				.withColumn(END_TIMESTAMP, DataTypes.TIMESTAMP)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(PARENT_ID, DataTypes.TIMEUUID)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql());
	}
	
	public void createTestEventMessagesLinkTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTestEventsMsgsLinkTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withClusteringColumn(TEST_EVENT_ID, DataTypes.UUID)
				.withClusteringColumn(MESSAGES_IDS, DataTypes.frozenListOf(DataTypes.TEXT));
		
		exec.executeQuery(create.asCql());
	}
}
