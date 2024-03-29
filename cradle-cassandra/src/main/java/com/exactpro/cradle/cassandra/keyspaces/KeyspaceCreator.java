/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public abstract class KeyspaceCreator {
	private static final Logger logger = LoggerFactory.getLogger(KeyspaceCreator.class);
	
	public static final long KEYSPACE_WAIT_TIMEOUT = 500;
	
	private final String keyspace;
	private final QueryExecutor queryExecutor;
	private final CassandraStorageSettings settings;
	private final long keyspaceReadinessTimeout;

	private KeyspaceMetadata keyspaceMetadata;
	
	public KeyspaceCreator(String keyspace, QueryExecutor queryExecutor, CassandraStorageSettings settings) {
		this.keyspace = keyspace;
		this.queryExecutor = queryExecutor;
		this.settings = settings;
		this.keyspaceReadinessTimeout = Math.max(KEYSPACE_WAIT_TIMEOUT, settings.getTimeout());
	}
	
	protected abstract void createTables() throws IOException;
	
	
	public void createAll() throws IOException, CradleStorageException {
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
	
	
	public void createKeyspace() throws IOException, CradleStorageException {

		Optional<KeyspaceMetadata> meta = obtainKeyspaceMetadata();
		if (meta.isPresent()) {
			logger.info("Keyspace \"{}\" already exists, skipping keyspace creation", keyspace);
			return;
		}

		logger.info("Creating keyspace \"{}\"", keyspace);
		CreateKeyspaceStart createKeyspace = SchemaBuilder.createKeyspace(keyspace).ifNotExists();

		String cql;
		if (settings.getNetworkTopologyStrategy() != null) {
			cql = createKeyspace.withNetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()).asCql();
		} else {
			cql = createKeyspace.withSimpleStrategy(settings.getKeyspaceReplicationFactor()).asCql();
		}
		queryExecutor.executeWrite(cql);
		logger.info("Keyspace \"{}\" has been created", keyspace);
		
		awaitKeyspaceReadiness();
	}


	private void awaitKeyspaceReadiness() throws CradleStorageException	{
		long timeRemaining = keyspaceReadinessTimeout;

		while (getKeyspaceMetadata() == null && timeRemaining > 0) {
			logger.debug("[{}] waiting {}ms for keyspace to be ready", timeRemaining, KEYSPACE_WAIT_TIMEOUT);
			try	{
				TimeUnit.MILLISECONDS.sleep(Math.min(KEYSPACE_WAIT_TIMEOUT, timeRemaining));
				timeRemaining -= KEYSPACE_WAIT_TIMEOUT;
			} catch (InterruptedException e) {
				throw new CradleStorageException(
						String.format("Interrupted while waiting for keyspace \"%s\" readiness", keyspace), e);
			}
		}
		
		if (getKeyspaceMetadata() == null)
			throw new CradleStorageException(
					String.format("Keyspace \"%s\" still not available after %d ms", keyspace, keyspaceReadinessTimeout));
	}

	
	protected boolean isTableExists(String tableName) {
		return getKeyspaceMetadata().getTable(tableName).isPresent();
	}


	protected boolean isIndexExists(String indexName, String tableName) {
		Optional<TableMetadata> tableMetadata = getKeyspaceMetadata().getTable(tableName);
		return tableMetadata.isPresent() && tableMetadata.get().getIndexes().containsKey(CqlIdentifier.fromCql(indexName));
	}


	protected KeyspaceMetadata getKeyspaceMetadata() {
		if (keyspaceMetadata != null)
			return keyspaceMetadata;
		
		obtainKeyspaceMetadata().ifPresent(value -> keyspaceMetadata = value);
		return keyspaceMetadata;
	}

	
	private Optional<KeyspaceMetadata> obtainKeyspaceMetadata()	{
		return queryExecutor.getSession().getMetadata().getKeyspace(keyspace);
	}

	
	protected boolean isColumnExists(String tableName, String columnName) {
		return getKeyspaceMetadata().getTable(tableName).get().getColumn(columnName).isPresent();
	}


	protected void createTable(String tableName, Supplier<CreateTable> query) throws IOException {
		if (isTableExists(tableName)) {
			logger.info("Table \"{}\" already exists, skipping creation", tableName);
			return;
		}
		
		logger.info("Creating table \"{}\"", tableName);
		queryExecutor.executeWrite(query.get().asCql());
		logger.info("Table \"{}\" has been created", tableName);
	}


	protected void createIndex(String indexName, String tableName, String columnName) throws IOException {
		if (isIndexExists(indexName, tableName)) {
			logger.info("Index \"{}\" already exists, skipping creation", indexName);
			return;
		}
		
		logger.info("Creating index \"{}\" on table \"{}\".\"{}\"", indexName, tableName, columnName);
		String query = SchemaBuilder
				.createIndex(indexName)
				.onTable(keyspace, tableName)
				.andColumn(columnName)
				.asCql();
		queryExecutor.executeWrite(query);
		logger.info("Index \"{}\" has been created", indexName);
	}
}
