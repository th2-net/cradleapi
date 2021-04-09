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

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraTablesCreator
{
	private static final Logger logger = LoggerFactory.getLogger(CassandraTablesCreator.class);

	protected final QueryExecutor exec;
	private KeyspaceMetadata keyspaceMetadata;
	protected final CassandraStorageSettings settings;
	
	public CassandraTablesCreator(QueryExecutor exec, CassandraStorageSettings settings)
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
		createTestEventsChildrenDatesTable();
		createTestEventsMessagesTable();
		createMessagesTestEventsTable();
		createStreamsTable();
		createRootTestEventsDatesTable();
	}

	public void createRootTestEventsDatesTable() throws IOException
	{
		createRootTestEventsDatesTable(settings.getRootTestEventsDatesTableName());
	}

	protected void createRootTestEventsDatesTable(String tableName) throws IOException
	{
		if (isTableExists(tableName))
			return;

		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withClusteringColumn(START_DATE, DataTypes.DATE)
				.withClusteringOrder(START_DATE, ClusteringOrder.ASC);

		exec.executeQuery(create.asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}

	public void createKeyspace() throws IOException
	{
		String keyspace = settings.getKeyspace();
		if (!isKeyspaceExists(keyspace))
		{
			CreateKeyspace createKs = settings.getNetworkTopologyStrategy() != null 
					? SchemaBuilder.createKeyspace(keyspace).ifNotExists().withNetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()) 
					: SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			exec.getSession().execute(createKs.build());
			logger.info("Keyspace '{}' has been created", keyspace);
		}
		doAfterKeyspaceCreating(keyspace);
	}

	protected void doAfterKeyspaceCreating(String keyspace)
	{
		this.keyspaceMetadata = getKeyspaceMetadata(keyspace).get();
	}

	protected boolean isKeyspaceExists(String keyspace) throws IOException
	{
		if (keyspaceMetadata != null)
			return true;

		if (!getKeyspaceMetadata(keyspace).isPresent())
			return false;
		
		return true;
	}

	public void createInstancesTable() throws IOException
	{
		String tableName = CassandraStorageSettings.INSTANCES_TABLE_DEFAULT_NAME;
		if (isTableExists(tableName))
			return;
		
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(NAME, DataTypes.TEXT)  //Name is a key for faster ID obtaining by name
				.withColumn(ID, DataTypes.UUID);
		
		createTable(create.asCql(), tableName);
	}
	
	public void createMessagesTable() throws IOException
	{
		createMessagesTable(settings.getMessagesTableName());
	}
	
	public void createStreamsTable() throws IOException
	{
		createStreamsTable(settings.getStreamsTableName());
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

		createTable(create.asCql(), tableName);
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

		createTable(create.asCql(), tableName);
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

		createTable(create.asCql(), tableName);
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

		createTable(create.asCql(), tableName);
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

		createTable(create.asCql(), tableName);
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

		createTable(create.asCql(), tableName);
	}
	
	public void createTestEventsMessagesTable() throws IOException
	{
		String tableName = settings.getTestEventsMessagesTableName();
		if (isTableExists(tableName))
			return;
		
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(TEST_EVENT_ID, DataTypes.TEXT)
				.withClusteringColumn(ID, DataTypes.TIMEUUID)
				.withColumn(MESSAGE_IDS, DataTypes.setOf(DataTypes.TEXT));

		createTable(create.asCql(), tableName);
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

		createTable(create.asCql(), tableName);
	}
	
	protected void createMessagesTable(String tableName) throws IOException
	{
		if (isTableExists(tableName))
			return;
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
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

		createTable(create.asCql(), tableName);
	}

	private void createStreamsTable(String tableName) throws IOException
	{
		if (isTableExists(tableName))
			return;
		
		CreateTableWithOptions create = SchemaBuilder.createTable(settings.getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(INSTANCE_ID, DataTypes.UUID)
				.withPartitionKey(STREAM_NAME, DataTypes.TEXT);

		createTable(create.asCql(), tableName);
	}

	protected boolean isTableExists(String tableName) throws IOException
	{
		return keyspaceMetadata.getTable(tableName).isPresent();
	}
	
	private Optional<KeyspaceMetadata> getKeyspaceMetadata(String keyspace)
	{
		return exec.getSession().getMetadata().getKeyspace(keyspace);
	}
	
	protected boolean isColumnExists(String tableName, String columnName) throws IOException
	{
		return keyspaceMetadata.getTable(tableName).get().getColumn(EVENT_BATCH_METADATA).isPresent();
	}

	private void createTable(String createQuery, String tableName) throws IOException
	{
		exec.executeQuery(createQuery, true);
		logger.info("Table '{}' has been created", tableName);
	}
}
