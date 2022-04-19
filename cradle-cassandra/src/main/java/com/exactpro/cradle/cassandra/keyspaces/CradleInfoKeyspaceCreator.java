/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.cassandra.dao.BooksStatusEntity;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CradleInfoKeyspaceCreator extends KeyspaceCreator
{
	private static final Logger logger = LoggerFactory.getLogger(CradleInfoKeyspaceCreator.class);
	
	public CradleInfoKeyspaceCreator(QueryExecutor exec, CassandraStorageSettings settings)
	{
		super(settings.getCradleInfoKeyspace(), exec, settings);
	}

	@Override
	protected void createTables() throws IOException
	{
		createBooks();
		createBooksStatus();
	}

	@Override
	public void createAll() throws IOException, CradleStorageException
	{
		if (getKeyspaceMetadata() != null)
		{
			logger.info("\"Cradle Info\" keyspace '{}' already exists", getKeyspace());
		}
		super.createAll();
	}

	private void createBooks() throws IOException
	{
		String tableName = getSettings().getBooksTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(BookEntity.FIELD_NAME, DataTypes.TEXT)
				.withColumn(BookEntity.FIELD_FULLNAME, DataTypes.TEXT)
				.withColumn(BookEntity.FIELD_KEYSPACE_NAME, DataTypes.TEXT)
				.withColumn(BookEntity.FIELD_DESCRIPTION, DataTypes.TEXT)
				.withColumn(BookEntity.FIELD_CREATED, DataTypes.TIMESTAMP)
				.withColumn(BookEntity.FIELD_SCHEMA_VERSION, DataTypes.TEXT));
	}

	private void createBooksStatus() throws IOException
	{
		String tableName = getSettings().getBooksStatusTable();
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(BooksStatusEntity.FIELD_BOOK_NAME, DataTypes.TEXT)
				.withClusteringColumn(BooksStatusEntity.FIELD_OBJECT_TYPE, DataTypes.TEXT)
				.withClusteringColumn(BooksStatusEntity.FIELD_OBJECT_NAME, DataTypes.TEXT)
				.withColumn(BooksStatusEntity.FIELD_CREATED, DataTypes.TIMESTAMP)
				.withColumn(BooksStatusEntity.FIELD_SCHEMA_VERSION, DataTypes.TEXT));
	}
}
