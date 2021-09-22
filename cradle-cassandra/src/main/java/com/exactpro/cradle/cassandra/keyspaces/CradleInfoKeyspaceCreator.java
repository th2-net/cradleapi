/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CradleInfoKeyspaceCreator extends KeyspaceCreator
{
	public CradleInfoKeyspaceCreator(QueryExecutor exec, CassandraStorageSettings settings)
	{
		super(settings.getCradleInfoKeyspace(), exec, settings);
	}
	
	@Override
	protected void createTables() throws IOException
	{
		createBooksTable();
		createScopesTable();
	}
	
	
	private void createBooksTable() throws IOException
	{
		String tableName = getSettings().getBooksTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(NAME, DataTypes.TEXT)
				.withColumn(FULLNAME, DataTypes.TEXT)
				.withColumn(KEYSPACE_NAME, DataTypes.TEXT)
				.withColumn(DESCRIPTION, DataTypes.TEXT)
				.withColumn(CREATED, DataTypes.TIMESTAMP));
	}
	
	private void createScopesTable() throws IOException
	{
		String tableName = getSettings().getScopesTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(BOOK, DataTypes.TEXT)
				.withClusteringColumn(SCOPE, DataTypes.TEXT));
	}
}
