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

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger logger = LoggerFactory.getLogger(TablesCreator.class);
	
	private final QueryExecutor exec;
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
		createProcessedMessagesTable();
		createTimeMessagesTable();
		createTestEventsTable();
		createTestEventMessagesLinkTable();
	}
	
	public void createKeyspace()
	{
		Optional<KeyspaceMetadata> keyspaceExist = exec.getSession().getMetadata().getKeyspace(settings.getKeyspace());
		if(!keyspaceExist.isPresent())
		{
			CreateKeyspace createKs = settings.getNetworkTopologyStrategy() != null 
					? SchemaBuilder.createKeyspace(settings.getKeyspace()).withNetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()) 
					: SchemaBuilder.createKeyspace(settings.getKeyspace()).withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			exec.getSession().execute(createKs.build());
			logger.info("Keyspace '{}' created", settings.getKeyspace());
		}
	}

	public void createInstancesTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), CassandraStorageSettings.INSTANCES_TABLE_DEFAULT_NAME).ifNotExists()
				.withPartitionKey(NAME, DataTypes.TEXT)  //Name is a key for faster ID obtaining by name
				.withColumn(ID, DataTypes.UUID);
		
		if (exec.executeQuery(create.asCql(), true).wasApplied())
			logger.info("Table '{}' created", CassandraStorageSettings.INSTANCES_TABLE_DEFAULT_NAME);
	}
	
	public void createMessagesTable() throws IOException
	{
		createMessagesTable(settings.getMessagesTableName());
	}
	
	public void createProcessedMessagesTable() throws IOException
	{
		createMessagesTable(settings.getProcessedMessagesTableName());
	}
	
	public void createTimeMessagesTable() throws IOException
	{
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTimeMessagesTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(STORED_TIMESTAMP, DataTypes.TIMESTAMP)
				.withPartitionKey(STREAM_NAME, DataTypes.TEXT)
				.withClusteringColumn(DIRECTION, DataTypes.TEXT)
				.withClusteringColumn(MESSAGE_INDEX, DataTypes.BIGINT)
				.withClusteringOrder(DIRECTION, ClusteringOrder.ASC)
				.withClusteringOrder(MESSAGE_INDEX, ClusteringOrder.ASC);
		
		if (exec.executeQuery(create.asCql(), true).wasApplied())
			logger.info("Table '{}' created", settings.getTimeMessagesTableName());
	}
	

	public void createTestEventsTable() throws IOException
	{
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTestEventsTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(ROOT, DataTypes.BOOLEAN)
				.withClusteringColumn(ID, DataTypes.TEXT)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(PARENT_ID, DataTypes.TEXT)
				.withColumn(EVENT_BATCH, DataTypes.BOOLEAN)
				.withColumn(STORED_DATE, DataTypes.DATE)
				.withColumn(STORED_TIME, DataTypes.TIME)
				.withColumn(START_DATE, DataTypes.DATE)
				.withColumn(START_TIME, DataTypes.TIME)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(EVENT_COUNT, DataTypes.INT)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		if (exec.executeQuery(create.asCql(), true).wasApplied())
			logger.info("Table '{}' created", settings.getTestEventsTableName());
	}
	
	//Many-to-many
	public void createTestEventMessagesLinkTable() throws IOException
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getTestEventMsgsLinkTableName()).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withClusteringColumn(TEST_EVENT_ID, DataTypes.TEXT)
				.withClusteringColumn(MESSAGES_IDS, DataTypes.frozenSetOf(DataTypes.TEXT))
				.withColumn(BATCH_ID, DataTypes.TEXT);
		
		if (exec.executeQuery(create.asCql(), true).wasApplied())
			logger.info("Table '{}' created", settings.getTestEventMsgsLinkTableName());
	}
	
	protected void createMessagesTable(String name) throws IOException
	{
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), name).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(STREAM_NAME, DataTypes.TEXT)
				.withClusteringColumn(DIRECTION, DataTypes.TEXT)
				.withClusteringColumn(MESSAGE_INDEX, DataTypes.BIGINT)
				.withColumn(STORED_DATE, DataTypes.DATE)
				.withColumn(STORED_TIME, DataTypes.TIME)
				.withColumn(FIRST_MESSAGE_DATE, DataTypes.DATE)
				.withColumn(FIRST_MESSAGE_TIME, DataTypes.TIME)
				.withColumn(LAST_MESSAGE_DATE, DataTypes.DATE)
				.withColumn(LAST_MESSAGE_TIME, DataTypes.TIME)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(MESSAGE_COUNT, DataTypes.INT)
				.withColumn(LAST_MESSAGE_INDEX, DataTypes.BIGINT)
				.withClusteringOrder(DIRECTION, ClusteringOrder.ASC)
				.withClusteringOrder(MESSAGE_INDEX, ClusteringOrder.ASC);
		
		if (exec.executeQuery(create.asCql(), true).wasApplied())
			logger.info("Table '{}' created", name);
	}
}
