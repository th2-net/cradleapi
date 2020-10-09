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
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumn;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class TablesCreator
{
	private static final Logger logger = LoggerFactory.getLogger(TablesCreator.class);
	
	private final QueryExecutor exec;
	private KeyspaceMetadata keyspaceMetadata;
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
		createTimeTestEventsTable();
		createRootTestEventsTable();
		createTestEventsChildrenTable();
		createTestEventsMessagesTable();
		createMessagesTestEventsTable();
	}
	
	public void createKeyspace()
	{
		Optional<KeyspaceMetadata> keyspaceExists = getKeyspaceMetadata();
		if (!keyspaceExists.isPresent())
		{
			CreateKeyspace createKs = settings.getNetworkTopologyStrategy() != null 
					? SchemaBuilder.createKeyspace(settings.getKeyspace()).withNetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()) 
					: SchemaBuilder.createKeyspace(settings.getKeyspace()).withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			exec.getSession().execute(createKs.build());
			logger.info("Keyspace '{}' has been created", settings.getKeyspace());
			this.keyspaceMetadata = getKeyspaceMetadata().get();
		}
		else
			this.keyspaceMetadata = keyspaceExists.get();
	}

	public void createInstancesTable() throws IOException
	{
		String tableName = CassandraStorageSettings.INSTANCES_TABLE_DEFAULT_NAME;
		if (isTableExists(tableName))
			return;
		
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(NAME, DataTypes.TEXT)  //Name is a key for faster ID obtaining by name
				.withColumn(ID, DataTypes.UUID);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
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
		String tableName = settings.getTimeMessagesTableName();
		if (isTableExists(tableName))
			return;
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(STREAM_NAME, DataTypes.TEXT)
				.withPartitionKey(DIRECTION, DataTypes.TEXT)
				.withPartitionKey(MESSAGE_DATE, DataTypes.DATE)
				.withClusteringColumn(MESSAGE_TIME, DataTypes.TIME)
				.withClusteringColumn(MESSAGE_INDEX, DataTypes.BIGINT)
				.withClusteringOrder(MESSAGE_TIME, ClusteringOrder.ASC)
				.withClusteringOrder(MESSAGE_INDEX, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	

	public void createTestEventsTable() throws IOException
	{
		String tableName = settings.getTestEventsTableName();
		if (isTableExists(tableName))
			return;
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(ID, DataTypes.TEXT)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(ROOT, DataTypes.BOOLEAN)
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
				.withColumn(EVENT_COUNT, DataTypes.INT);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	
	public void createTimeTestEventsTable() throws IOException
	{
		String tableName = settings.getTimeTestEventsTableName();
		if (isTableExists(tableName))
		{
			if (!isColumnExists(tableName, EVENT_BATCH_METADATA))
			{
				AlterTableAddColumnEnd alter = SchemaBuilder.alterTable(settings.getKeyspace(), tableName).addColumn(EVENT_BATCH_METADATA, DataTypes.BLOB);
				exec.executeQuery(alter.asCql(), true);
				logger.info("Table '{}' has been altered with column '{}'", tableName, EVENT_BATCH_METADATA);
			}
			return;
		}
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(START_DATE, DataTypes.DATE)
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withClusteringColumn(ID, DataTypes.TEXT)
				.withColumn(ROOT, DataTypes.BOOLEAN)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(PARENT_ID, DataTypes.TEXT)
				.withColumn(EVENT_BATCH, DataTypes.BOOLEAN)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(EVENT_COUNT, DataTypes.INT)
				.withColumn(EVENT_BATCH_METADATA, DataTypes.BLOB)
				.withClusteringOrder(START_TIME, ClusteringOrder.ASC)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	
	public void createRootTestEventsTable() throws IOException
	{
		String tableName = settings.getRootTestEventsTableName();
		if (isTableExists(tableName))
			return;
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(START_DATE, DataTypes.DATE)
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withClusteringColumn(ID, DataTypes.TEXT)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(EVENT_BATCH, DataTypes.BOOLEAN)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(EVENT_COUNT, DataTypes.INT)
				.withClusteringOrder(START_TIME, ClusteringOrder.ASC)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	
	public void createTestEventsChildrenTable() throws IOException
	{
		String tableName = settings.getTestEventsChildrenTableName();
		if (isTableExists(tableName))
		{
			if (!isColumnExists(tableName, EVENT_BATCH_METADATA))
			{
				AlterTableAddColumnEnd alter = SchemaBuilder.alterTable(settings.getKeyspace(), tableName).addColumn(EVENT_BATCH_METADATA, DataTypes.BLOB);
				exec.executeQuery(alter.asCql(), true);
				logger.info("Table '{}' has been altered with column '{}'", tableName, EVENT_BATCH_METADATA);
			}
			return;
		}
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(PARENT_ID, DataTypes.TEXT)
				.withPartitionKey(START_DATE, DataTypes.DATE)
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withClusteringColumn(ID, DataTypes.TEXT)
				.withColumn(ROOT, DataTypes.BOOLEAN)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(EVENT_BATCH, DataTypes.BOOLEAN)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(EVENT_COUNT, DataTypes.INT)
				.withColumn(EVENT_BATCH_METADATA, DataTypes.BLOB)
				.withClusteringOrder(START_TIME, ClusteringOrder.ASC)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	
	public void createTestEventsMessagesTable() throws IOException
	{
		String tableName = settings.getTestEventsMessagesTableName();
		if (isTableExists(tableName))
			return;
		
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(TEST_EVENT_ID, DataTypes.TEXT)
				.withClusteringColumn(MESSAGE_IDS, DataTypes.frozenSetOf(DataTypes.TEXT));
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	
	public void createMessagesTestEventsTable() throws IOException
	{
		String tableName = settings.getMessagesTestEventsTableName();
		if (isTableExists(tableName))
			return;
		
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(MESSAGE_ID, DataTypes.TEXT)
				.withClusteringColumn(TEST_EVENT_ID, DataTypes.TEXT)
				.withColumn(BATCH_ID, DataTypes.TEXT);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	
	protected void createMessagesTable(String name) throws IOException
	{
		if (isTableExists(name))
			return;
		
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
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", name);
	}
	
	
	private boolean isTableExists(String tableName)
	{
		return keyspaceMetadata.getTable(tableName).isPresent();
	}
	
	private Optional<KeyspaceMetadata> getKeyspaceMetadata()
	{
		return exec.getSession().getMetadata().getKeyspace(settings.getKeyspace());
	}
	
	private boolean isColumnExists(String tableName, String columnName)
	{
		return keyspaceMetadata.getTable(tableName).get().getColumn(EVENT_BATCH_METADATA).isPresent();
	}
}
