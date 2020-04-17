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
		createMessagesTable();
		createStreamMessagesLinkTable();
		
		createTestEventsTable();
		createTestEventsParentsLinkTable();
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
				.withPartitionKey(NAME, DataTypes.TEXT)  //Name is a key for faster ID obtaining by name
				.withColumn(ID, DataTypes.UUID);
		
		exec.executeQuery(create.asCql());
	}
	
	public void createMessagesTable() throws IOException
	{
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getMessagesTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withClusteringColumn(ID, DataTypes.TEXT)
				.withColumn(STORED, DataTypes.TIMESTAMP)
				.withColumn(DIRECTION, DataTypes.TEXT)
				.withColumn(TIMESTAMP, DataTypes.TIMESTAMP)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(BATCH_SIZE, DataTypes.INT)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql());
	}
	
	public void createStreamMessagesLinkTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getStreamMsgsLinkTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				//Not too many streams are expected and no requests to this table only by message ID, so can make stream name the partition key
				.withPartitionKey(STREAM_NAME, DataTypes.TEXT)
				.withClusteringColumn(MESSAGES_IDS, DataTypes.frozenListOf(DataTypes.TEXT));
		
		exec.executeQuery(create.asCql());
	}
	

	public void createTestEventsTable() throws IOException
	{
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTestEventsTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(IS_ROOT, DataTypes.BOOLEAN)
				.withClusteringColumn(ID, DataTypes.TEXT)
				.withColumn(STORED, DataTypes.TIMESTAMP)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(BATCH_SIZE, DataTypes.INT)
				.withColumn(PARENT_ID, DataTypes.TEXT)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql());
	}
	
	//This table is needed to link test events with their parents. This allows to get IDs of test events bound to particular parent ID.
	//Making parent ID the part of a primary key in test events table would prevent from getting test event data only by event ID
	//One-to-many
	public void createTestEventsParentsLinkTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTestEventsParentsLinkTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(PARENT_ID, DataTypes.TEXT)
				.withClusteringColumn(BATCH_ID, DataTypes.TEXT)
				.withColumn(BATCH_SIZE, DataTypes.INT);
		
		exec.executeQuery(create.asCql());
	}
	
	//Many-to-many
	public void createTestEventMessagesLinkTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTestEventMsgsLinkTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withClusteringColumn(TEST_EVENT_ID, DataTypes.TEXT)
				.withClusteringColumn(MESSAGES_IDS, DataTypes.frozenListOf(DataTypes.TEXT));
		
		exec.executeQuery(create.asCql());
	}
}
