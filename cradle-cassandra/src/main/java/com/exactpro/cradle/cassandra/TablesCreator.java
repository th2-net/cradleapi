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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class TablesCreator
{
	private static final Logger logger = LoggerFactory.getLogger(TablesCreator.class);
	
	private final String keyspace;
	private final QueryExecutor exec;
	private final CassandraStorageSettings settings;
	
	private KeyspaceMetadata keyspaceMetadata;
	
	public TablesCreator(String keyspace, QueryExecutor exec, CassandraStorageSettings settings)
	{
		this.keyspace = keyspace;
		this.exec = exec;
		this.settings = settings;
	}
	
	public void createAll() throws IOException
	{
		createKeyspace();
		createPagesTable();
		createMessagesTable();
		createSessionsTable();
		createSessionsDatesTable();
		createTestEventsTable();
		createScopesTable();
		createTestEventsDatesTable();
		createLabelsTable();
		createIntervalsTable();
	}
	
	public void createKeyspace()
	{
		Optional<KeyspaceMetadata> meta = obtainKeyspaceMetadata();
		if (!meta.isPresent())
		{
			CreateKeyspace createKs = settings.getNetworkTopologyStrategy() != null 
					? SchemaBuilder.createKeyspace(keyspace).withNetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()) 
					: SchemaBuilder.createKeyspace(keyspace).withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			exec.getSession().execute(createKs.build());
			logger.info("Keyspace '{}' has been created", keyspace);
			this.keyspaceMetadata = obtainKeyspaceMetadata().get();  //FIXME: keyspace creation may take etime and won't be available immediately
		}
		else
			this.keyspaceMetadata = meta.get();
	}

	public void createPagesTable() throws IOException
	{
		String tableName = settings.getPagesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PART, DataTypes.TEXT)
				.withClusteringColumn(START_DATE, DataTypes.DATE)
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME));
	}
	
	public void createMessagesTable() throws IOException
	{
		String tableName = settings.getMessagesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(MESSAGE_DATE, DataTypes.DATE)
				.withPartitionKey(SESSION_ALIAS, DataTypes.TEXT)
				.withPartitionKey(DIRECTION, DataTypes.TEXT)
				.withPartitionKey(PART, DataTypes.TEXT)
				
				.withClusteringColumn(MESSAGE_TIME, DataTypes.TIME)
				.withClusteringColumn(SEQUENCE, DataTypes.BIGINT)
				.withClusteringColumn(CHUNK, DataTypes.INT)
				
				.withColumn(STORED_DATE, DataTypes.DATE)
				.withColumn(STORED_TIME, DataTypes.TIME)
				.withColumn(LAST_MESSAGE_DATE, DataTypes.DATE)
				.withColumn(LAST_MESSAGE_TIME, DataTypes.TIME)
				.withColumn(LAST_SEQUENCE, DataTypes.BIGINT)
				.withColumn(MESSAGE_COUNT, DataTypes.INT)
				.withColumn(LAST_CHUNK, DataTypes.BOOLEAN)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(LABELS, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(CONTENT, DataTypes.BLOB));
	}
	
	public void createSessionsTable() throws IOException
	{
		String tableName = settings.getSessionsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(SESSION_ALIAS, DataTypes.TEXT));
	}
	
	public void createSessionsDatesTable() throws IOException
	{
		String tableName = settings.getSessionsDatesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(MESSAGE_DATE, DataTypes.DATE)
				
				.withClusteringColumn(SESSION_ALIAS, DataTypes.TEXT)
				.withClusteringColumn(DIRECTION, DataTypes.TEXT)
				.withClusteringColumn(PART, DataTypes.TEXT));
	}
	
	public void createTestEventsTable() throws IOException
	{
		String tableName = settings.getTestEventsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(START_DATE, DataTypes.DATE)
				.withPartitionKey(SCOPE, DataTypes.TEXT)
				.withPartitionKey(PART, DataTypes.TEXT)
				
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withClusteringColumn(ID, DataTypes.TEXT)
				.withClusteringColumn(CHUNK, DataTypes.INT)
				
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(ROOT, DataTypes.BOOLEAN)
				.withColumn(PARENT_ID, DataTypes.TEXT)
				.withColumn(EVENT_BATCH, DataTypes.BOOLEAN)
				.withColumn(EVENT_COUNT, DataTypes.INT)
				.withColumn(STORED_DATE, DataTypes.DATE)
				.withColumn(STORED_TIME, DataTypes.TIME)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME)
				.withColumn(LAST_CHUNK, DataTypes.BOOLEAN)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(MESSAGES, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(LABELS, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(CONTENT, DataTypes.BLOB));
	}
	
	public void createScopesTable() throws IOException
	{
		String tableName = settings.getTestEventsDatesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(SCOPE, DataTypes.TEXT));
	}
	
	public void createTestEventsDatesTable() throws IOException
	{
		String tableName = settings.getTestEventsDatesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(START_DATE, DataTypes.DATE)
				.withClusteringColumn(SCOPE, DataTypes.TEXT)
				.withClusteringColumn(PART, DataTypes.TEXT));
	}
	
	public void createLabelsTable() throws IOException
	{
		String tableName = settings.getLabelsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(NAME, DataTypes.TEXT));
	}
	
	public void createIntervalsTable() throws IOException
	{
		String tableName = settings.getIntervalsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(keyspace, tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
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
				.withColumn(INTERVAL_PROCESSED, DataTypes.BOOLEAN));
	}
	
	
	protected boolean isTableExists(String tableName)
	{
		return keyspaceMetadata.getTable(tableName).isPresent();
	}
	
	protected KeyspaceMetadata getKeyspaceMetadata()
	{
		if (keyspaceMetadata != null)
			return keyspaceMetadata;
		
		Optional<KeyspaceMetadata> metadata = obtainKeyspaceMetadata();
		if (metadata.isPresent())
			keyspaceMetadata = metadata.get();
		return keyspaceMetadata;
	}
	
	private Optional<KeyspaceMetadata> obtainKeyspaceMetadata()
	{
		return exec.getSession().getMetadata().getKeyspace(keyspace);
	}
	
	protected boolean isColumnExists(String tableName, String columnName)
	{
		return keyspaceMetadata.getTable(tableName).get().getColumn(columnName).isPresent();
	}
	
	private void createTable(String tableName, Supplier<CreateTable> query) throws IOException
	{
		if (isTableExists(tableName))
			return;
		
		exec.executeQuery(query.get().asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
}
