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

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public abstract class KeyspaceCreator
{
	private static final Logger logger = LoggerFactory.getLogger(KeyspaceCreator.class);
	
	private final String keyspace;
	private final QueryExecutor queryExecutor;
	private final CassandraStorageSettings settings;
	
	private KeyspaceMetadata keyspaceMetadata;
	
	public KeyspaceCreator(String keyspace, QueryExecutor queryExecutor, CassandraStorageSettings settings)
	{
		this.keyspace = keyspace;
		this.queryExecutor = queryExecutor;
		this.settings = settings;
	}
	
	
	protected abstract void createTables() throws IOException;
	
	
	public void createAll() throws IOException
	{
		createKeyspace();
		createTables();
	}
	
	
	public String getKeyspace()
	{
		return keyspace;
	}
	
	public QueryExecutor getQueryExecutor()
	{
		return queryExecutor;
	}
	
	public CassandraStorageSettings getSettings()
	{
		return settings;
	}
	
	
	public void createKeyspace()
	{
		Optional<KeyspaceMetadata> meta = obtainKeyspaceMetadata();
		if (!meta.isPresent())
		{
			logger.info("Creating keyspace '{}'", keyspace);
			CreateKeyspace createKs = settings.getNetworkTopologyStrategy() != null 
					? SchemaBuilder.createKeyspace(keyspace).withNetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()) 
					: SchemaBuilder.createKeyspace(keyspace).withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			queryExecutor.getSession().execute(createKs.build().setTimeout(Duration.ofMillis(settings.getTimeout())));
			logger.info("Keyspace '{}' has been created", keyspace);
			this.keyspaceMetadata = obtainKeyspaceMetadata().get();  //FIXME: keyspace creation may take time and it won't be available immediately
		}
		else
		{
			logger.info("Keyspace '{}' already exists", keyspace);
			this.keyspaceMetadata = meta.get();
		}
	}
	
	protected boolean isTableExists(String tableName)
	{
		return keyspaceMetadata.getTable(tableName).isPresent();
	}
	
	protected boolean isIndexExists(String indexName, String tableName)
	{
		Optional<TableMetadata> tableMetadata = keyspaceMetadata.getTable(tableName);
		return tableMetadata.isPresent() && tableMetadata.get().getIndexes().containsKey(CqlIdentifier.fromCql(indexName));
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
		return queryExecutor.getSession().getMetadata().getKeyspace(keyspace);
	}
	
	protected boolean isColumnExists(String tableName, String columnName)
	{
		return keyspaceMetadata.getTable(tableName).get().getColumn(columnName).isPresent();
	}
	
	protected void createTable(String tableName, Supplier<CreateTable> query) throws IOException
	{
		if (isTableExists(tableName))
		{
			logger.info("Table '{}' already exists", tableName);
			return;
		}
		
		logger.info("Creating table '{}'", tableName);
		queryExecutor.executeQuery(query.get().asCql(), true);
		logger.info("Table '{}' has been created", tableName);
	}
	
	protected void createIndex(String indexName, String tableName, String columnName) throws IOException
	{
		if (isIndexExists(indexName, tableName))
		{
			logger.info("Index '{}' already exists", indexName);
			return;
		}
		
		logger.info("Creating index '{}' for {}.{}", indexName, tableName, columnName);
		queryExecutor.executeQuery(SchemaBuilder.createIndex(indexName)
				.onTable(keyspace, tableName).andColumn(columnName).asCql(), true);
		logger.info("Index '{}' for {}.{} has been created", indexName, tableName, columnName);
	}
}
