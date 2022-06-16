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

import com.datastax.oss.driver.api.querybuilder.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
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
		createGroupedMessagesTable();
		createProcessedMessagesTable();
		createTimeMessagesTable();
		createTestEventsTable();
		createTestEventsChildrenDatesTable();
		createTimeTestEventsTable();
		createIntervalsTable();
		createIndexes();
	}

	private void createIndexes() throws IOException
	{
		String tableName = settings.getTimeTestEventsTableName();
		String indexName = PARENT_ID + INDEX_NAME_POSTFIX;
		CreateIndex createIndex = SchemaBuilder.createIndex(indexName).ifNotExists()
				.onTable(settings.getKeyspace(), tableName).andColumn(PARENT_ID);
		exec.executeQuery(createIndex.asCql(), true);
		logger.info("Index '{}' on table '{}' and column '{}' has been created", indexName, tableName, PARENT_ID);
	}

	public void createKeyspace() throws IOException
	{
		Optional<KeyspaceMetadata> keyspaceExists = getKeyspaceMetadata();
		if (!keyspaceExists.isPresent())
		{
			CreateKeyspace createKs = settings.getNetworkTopologyStrategy() != null 
					? SchemaBuilder.createKeyspace(settings.getKeyspace()).withNetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()) 
					: SchemaBuilder.createKeyspace(settings.getKeyspace()).withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			exec.executeQuery(createKs.asCql(), true);
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
				.withClusteringColumn(MESSAGE_DATE, DataTypes.DATE)
				.withClusteringColumn(MESSAGE_TIME, DataTypes.TIME)
				.withClusteringColumn(MESSAGE_INDEX, DataTypes.BIGINT)
				.withClusteringOrder(MESSAGE_DATE, ClusteringOrder.ASC)
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
				.withColumn(START_DATE, DataTypes.DATE)
				.withColumn(START_TIME, DataTypes.TIME);
		
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
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(STORED_DATE, DataTypes.DATE)
				.withColumn(STORED_TIME, DataTypes.TIME)
				.withColumn(MESSAGE_IDS, DataTypes.BLOB)
				.withClusteringOrder(START_TIME, ClusteringOrder.ASC)
				.withClusteringOrder(ID, ClusteringOrder.ASC);
		
		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}

	public void createTestEventsChildrenDatesTable() throws IOException
	{
		String tableName = settings.getTestEventsChildrenDatesTableName();
		if (isTableExists(tableName))
			return;

		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(PARENT_ID, DataTypes.TEXT)
				.withClusteringColumn(START_DATE, DataTypes.DATE)
				.withClusteringOrder(START_DATE, ClusteringOrder.ASC);

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
	
	public void createGroupedMessagesTable() throws IOException
	{
		String name = settings.getGroupedMessagesTableName();
		if (isTableExists(name))
			return;
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), name).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(STREAM_GROUP, DataTypes.TEXT)

				.withClusteringColumn(MESSAGE_DATE, DataTypes.DATE)
				.withClusteringColumn(MESSAGE_TIME, DataTypes.TIME)

				.withColumn(LAST_MESSAGE_DATE, DataTypes.DATE)
				.withColumn(LAST_MESSAGE_TIME, DataTypes.TIME)
				.withColumn(STORED_DATE, DataTypes.DATE)
				.withColumn(STORED_TIME, DataTypes.TIME)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(CONTENT, DataTypes.BLOB)
				.withColumn(MESSAGE_COUNT, DataTypes.INT)

				.withClusteringOrder(MESSAGE_DATE, ClusteringOrder.ASC)
				.withClusteringOrder(MESSAGE_TIME, ClusteringOrder.ASC);

		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", name);
	}
	
	public void createIntervalsTable() throws IOException
	{
		String tableName = settings.getIntervalsTableName();
		if (isTableExists(tableName))
			return;

		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(INTERVAL_START_DATE, DataTypes.DATE)
				.withClusteringColumn(CRAWLER_NAME, DataTypes.TEXT)
				.withClusteringColumn(CRAWLER_VERSION, DataTypes.TEXT)
				.withClusteringColumn(CRAWLER_TYPE, DataTypes.TEXT)
				.withClusteringColumn(INTERVAL_START_TIME, DataTypes.TIME)
				.withColumn(INTERVAL_END_DATE, DataTypes.DATE)
				.withColumn(INTERVAL_END_TIME, DataTypes.TIME)
				.withColumn(INTERVAL_LAST_UPDATE_DATE, DataTypes.DATE)
				.withColumn(INTERVAL_LAST_UPDATE_TIME, DataTypes.TIME)
				.withColumn(RECOVERY_STATE_JSON, DataTypes.TEXT)
				.withColumn(INTERVAL_PROCESSED, DataTypes.BOOLEAN);

		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
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
