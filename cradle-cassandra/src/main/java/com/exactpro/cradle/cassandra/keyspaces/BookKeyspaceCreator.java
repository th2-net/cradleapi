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

package com.exactpro.cradle.cassandra.keyspaces;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

import java.io.IOException;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class BookKeyspaceCreator extends KeyspaceCreator
{

	public BookKeyspaceCreator(String keyspace, QueryExecutor exec, CassandraStorageSettings settings)
	{
		super(keyspace, exec, settings);
	}
	
	@Override
	protected void createTables() throws IOException
	{
		createPages();
		createPagesNames();
		
		createSessions();
		createScopes();
		
		createMessages();
		createPageSessions();
		
		createTestEvents();
		createPageScopes();
		createTestEventParentIndex();
		
		createLabelsTable();
		createIntervals();

		createMessageStatistics();
		createEntityStatistics();
	}


	private void createPages() throws IOException
	{
		String tableName = getSettings().getPagesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PART, DataTypes.TEXT)
				.withClusteringColumn(START_DATE, DataTypes.DATE)
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(COMMENT, DataTypes.TEXT)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME)
				.withColumn(REMOVED, DataTypes.TIMESTAMP));
	}
	
	private void createPagesNames() throws IOException
	{
		String tableName = getSettings().getPagesNamesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PART, DataTypes.TEXT)
				.withPartitionKey(NAME, DataTypes.TEXT)
				.withColumn(START_DATE, DataTypes.DATE)
				.withColumn(START_TIME, DataTypes.TIME)
				.withColumn(COMMENT, DataTypes.TEXT)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME));
	}
	
	private void createSessions() throws IOException
	{
		String tableName = getSettings().getSessionsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PART, DataTypes.TEXT)
				.withClusteringColumn(SESSION_ALIAS, DataTypes.TEXT));
	}
	
	private void createScopes() throws IOException
	{
		String tableName = getSettings().getScopesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PART, DataTypes.TEXT)
				.withClusteringColumn(SCOPE, DataTypes.TEXT));
	}

	private void createMessages() throws IOException
	{
		String tableName = getSettings().getMessagesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(SESSION_ALIAS, DataTypes.TEXT)
				.withPartitionKey(DIRECTION, DataTypes.TEXT)

				.withClusteringColumn(MESSAGE_DATE, DataTypes.DATE)
				.withClusteringColumn(MESSAGE_TIME, DataTypes.TIME)
				.withClusteringColumn(SEQUENCE, DataTypes.BIGINT)
				
				.withColumn(LAST_MESSAGE_DATE, DataTypes.DATE)
				.withColumn(LAST_MESSAGE_TIME, DataTypes.TIME)
				.withColumn(LAST_SEQUENCE, DataTypes.BIGINT)
				.withColumn(MESSAGE_COUNT, DataTypes.INT)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(LABELS, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(CONTENT, DataTypes.BLOB));
	}

	private void createPageSessions() throws IOException
	{
		String tableName = getSettings().getPageSessionsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)

				.withClusteringColumn(SESSION_ALIAS, DataTypes.TEXT)
				.withClusteringColumn(DIRECTION, DataTypes.TEXT));
	}
	
	private void createTestEvents() throws IOException
	{
		String tableName = getSettings().getTestEventsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(SCOPE, DataTypes.TEXT)
				
				.withClusteringColumn(START_DATE, DataTypes.DATE)
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withClusteringColumn(ID, DataTypes.TEXT)
				
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(TYPE, DataTypes.TEXT)
				.withColumn(SUCCESS, DataTypes.BOOLEAN)
				.withColumn(ROOT, DataTypes.BOOLEAN)
				.withColumn(PARENT_ID, DataTypes.TEXT)
				.withColumn(EVENT_BATCH, DataTypes.BOOLEAN)
				.withColumn(EVENT_COUNT, DataTypes.INT)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(MESSAGES, DataTypes.BLOB)
				.withColumn(LABELS, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(CONTENT, DataTypes.BLOB));
	}
	
	private void createPageScopes() throws IOException
	{
		String tableName = getSettings().getPageScopesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(SCOPE, DataTypes.TEXT));
	}
	
	private void createTestEventParentIndex() throws IOException
	{
		CassandraStorageSettings settings = getSettings();
		createIndex(settings.getTestEventParentIndex(), settings.getTestEventsTable(), PARENT_ID);
	}
	
	private void createLabelsTable() throws IOException
	{
		String tableName = getSettings().getLabelsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(NAME, DataTypes.TEXT));
	}
	
	private void createIntervals() throws IOException
	{
		String tableName = getSettings().getIntervalsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
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

	private void createMessageStatistics() throws IOException
	{
		String tableName = getSettings().getMessageStatisticsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(SESSION_ALIAS, DataTypes.TEXT)
				.withPartitionKey(DIRECTION, DataTypes.TEXT)
				.withPartitionKey(FRAME_TYPE, DataTypes.TINYINT)
				.withClusteringColumn(FRAME_START, DataTypes.TIMESTAMP)
				.withColumn(ENTITY_COUNT, DataTypes.COUNTER)
				.withColumn(ENTITY_SIZE, DataTypes.COUNTER));
	}

	private void createEntityStatistics() throws IOException
	{
		String tableName = getSettings().getEntityStatisticsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(ENTITY_TYPE, DataTypes.TINYINT)
				.withPartitionKey(FRAME_TYPE, DataTypes.TINYINT)
				.withClusteringColumn(FRAME_START, DataTypes.TIMESTAMP)
				.withColumn(ENTITY_COUNT, DataTypes.COUNTER)
				.withColumn(ENTITY_SIZE, DataTypes.COUNTER));
	}

}
