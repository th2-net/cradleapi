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

import java.io.IOException;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

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
	
	
	private void createPagesTable() throws IOException
	{
		String tableName = getSettings().getPagesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PART, DataTypes.TEXT)
				.withClusteringColumn(START_DATE, DataTypes.DATE)
				.withClusteringColumn(START_TIME, DataTypes.TIME)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(COMMENT, DataTypes.TEXT)
				.withColumn(END_DATE, DataTypes.DATE)
				.withColumn(END_TIME, DataTypes.TIME));
	}
	
	private void createMessagesTable() throws IOException
	{
		String tableName = getSettings().getMessagesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
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
	
	private void createSessionsTable() throws IOException
	{
		String tableName = getSettings().getSessionsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(SESSION_ALIAS, DataTypes.TEXT));
	}
	
	private void createSessionsDatesTable() throws IOException
	{
		String tableName = getSettings().getSessionsDatesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(MESSAGE_DATE, DataTypes.DATE)
				
				.withClusteringColumn(SESSION_ALIAS, DataTypes.TEXT)
				.withClusteringColumn(DIRECTION, DataTypes.TEXT)
				.withClusteringColumn(PART, DataTypes.TEXT));
	}
	
	private void createTestEventsTable() throws IOException
	{
		String tableName = getSettings().getTestEventsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
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
	
	private void createScopesTable() throws IOException
	{
		String tableName = getSettings().getScopesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(SCOPE, DataTypes.TEXT));
	}
	
	private void createTestEventsDatesTable() throws IOException
	{
		String tableName = getSettings().getTestEventsDatesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withPartitionKey(START_DATE, DataTypes.DATE)
				.withClusteringColumn(SCOPE, DataTypes.TEXT)
				.withClusteringColumn(PART, DataTypes.TEXT));
	}
	
	private void createLabelsTable() throws IOException
	{
		String tableName = getSettings().getLabelsTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PAGE, DataTypes.TEXT)
				.withClusteringColumn(NAME, DataTypes.TEXT));
	}
	
	private void createIntervalsTable() throws IOException
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
}