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

package com.exactpro.cradle.cassandra.amazon;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.CassandraTablesCreator;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class AmazonTablesCreator extends CassandraTablesCreator
{
	private static final Logger logger = LoggerFactory.getLogger(AmazonTablesCreator.class);
	
	public static final String AMAZON_SYSTEM_KEYSPACE = "system_schema_mcs";
	
	public static final String KEYSPACES_TABLE = "keyspaces";
	public static final String KEYSPACES_COLUMN = "keyspace_name";
	
	public static final String TABLES_TABLE = "tables";
	public static final String TABLE_NAME_COLUMN = "table_name";
	public static final String STATUS_COLUMN = "status";
	public static final String ACTIVE_STATUS = "ACTIVE";
	public static final String COLUMNS_TABLE = "columns";
	public static final String COLUMN_NAME_COLUMN = "column_name";
	public static final int ATTEMPTS_TO_CHECK_AVAILABILITY = 10;
	public static final int CHECK_AVAILABILITY_TIMEOUT_MS = 5000;

	private String currentKeyspace;
	
	private final String keyspaceQuery = QueryBuilder.selectFrom(AMAZON_SYSTEM_KEYSPACE, KEYSPACES_TABLE)
			.all()
			.whereColumn(KEYSPACES_COLUMN).isEqualTo(QueryBuilder.bindMarker())
			.limit(1)
			.asCql();

	private final String tableStatusQuery = QueryBuilder.selectFrom(AMAZON_SYSTEM_KEYSPACE, TABLES_TABLE)
			.all()
			.whereColumn(KEYSPACES_COLUMN).isEqualTo(QueryBuilder.bindMarker())
			.whereColumn(TABLE_NAME_COLUMN).isEqualTo(QueryBuilder.bindMarker())
			.limit(1)
			.asCql();

	private final String columnQuery = QueryBuilder.selectFrom(AMAZON_SYSTEM_KEYSPACE, COLUMNS_TABLE)
			.all()
			.whereColumn(KEYSPACES_COLUMN).isEqualTo(QueryBuilder.bindMarker())
			.whereColumn(TABLE_NAME_COLUMN).isEqualTo(QueryBuilder.bindMarker())
			.asCql();

	public AmazonTablesCreator(QueryExecutor exec, CassandraStorageSettings settings)
	{
		super(exec, settings);
	}

	@Override
	protected boolean isKeyspaceExists(String keyspace) throws IOException
	{
		return isKeyspaceAvailable(keyspace);
	}

	@Override
	protected boolean isTableExists(String tableName) throws IOException
	{
		return getTableStatus(tableName) != null;
	}

	@Override
	protected boolean isColumnExists(String tableName, String columnName) throws IOException
	{
		// We cannot use the 'column_name' column in where clause in Amazon keyspaces.
		ResultSet rs = exec.executeQuery(columnQuery, false, currentKeyspace, tableName);
		for (Row row : rs)
		{
			if (StringUtils.equals(row.getString(COLUMN_NAME_COLUMN), columnName))
				return true;
		}

		return false;
	}

	@Override
	protected void doAfterKeyspaceCreating(String keyspace)
	{
		// Amazon keyspaces stores the keyspace name in lowercase.
		// We must use lower case for queries to work properly.
		currentKeyspace = StringUtils.lowerCase(keyspace);
	}

	protected String getTableStatus(String tableName) throws IOException
	{
		ResultSet rs = exec.executeQuery(tableStatusQuery, false, currentKeyspace, tableName);
		Row row = rs.one();
		
		return row == null ? null : row.getString(STATUS_COLUMN); 
	}

	@Override
	public void createKeyspace() throws IOException
	{
		super.createKeyspace();
		waitKeyspaceAvailability();
	}

	private void waitKeyspaceAvailability() throws IOException
	{
		for (int i = 1; i <= ATTEMPTS_TO_CHECK_AVAILABILITY; i++)
		{
			logger.debug("Attempt {} to check readiness of the keyspace '{}'", i, currentKeyspace);
			if (isKeyspaceAvailable(currentKeyspace))
			{
				logger.debug("Kyespace '{}' is ready to use", currentKeyspace);
				return;
			}
			
			if (i == ATTEMPTS_TO_CHECK_AVAILABILITY)
				throw new IOException("Keyspace " + currentKeyspace + " is not available");
			
			logger.debug("Keyspace '{}' is not ready yet", currentKeyspace);
			
			try
			{
				Thread.sleep(CHECK_AVAILABILITY_TIMEOUT_MS);
			}
			catch (InterruptedException e)
			{
				throw new IOException(e);
			}
		}
	}

	protected boolean isKeyspaceAvailable(String keyspace) throws IOException
	{
		ResultSet rs = exec.executeQuery(keyspaceQuery, false, keyspace);
		Row row = rs.one();

		return row != null;
	}

	@Override
	public void createAll() throws IOException
	{
		super.createAll();
		waitTablesAvailability(Arrays.asList(
				settings.getInstanceTableName(),
				settings.getStreamsTableName(),
				settings.getMessagesTableName(),
				settings.getProcessedMessagesTableName(),
				settings.getTimeMessagesTableName(),
				settings.getTestEventsTableName(),
				settings.getTimeTestEventsTableName(),
				settings.getRootTestEventsTableName(),
				settings.getTestEventsChildrenTableName(),
				settings.getTestEventsChildrenDatesTableName(),
				settings.getTestEventsMessagesTableName(),
				settings.getMessagesTestEventsTableName(),
				settings.getRootTestEventsDatesTableName()));
	}

	private void waitTablesAvailability(Collection<String> tablesNames) throws IOException
	{
		for (String tableName : tablesNames)
		{
			for (int i = 1; i <= ATTEMPTS_TO_CHECK_AVAILABILITY; i++)
			{
				String currentStatus;
				logger.debug("Attempt {} to get status of the table '{}'", i, tableName);
				if (ACTIVE_STATUS.equals(currentStatus = getTableStatus(tableName)))
				{
					logger.debug("Table '{}' is {}", tableName, currentStatus);
					break;
				}
				logger.debug("Current status of table '{}' is '{}'", tableName, currentStatus);
				if (i == ATTEMPTS_TO_CHECK_AVAILABILITY)
					throw new IOException("Table '" + tableName + "' is not available");
				
				try
				{
					Thread.sleep(CHECK_AVAILABILITY_TIMEOUT_MS);
				}
				catch (InterruptedException e)
				{
					throw new IOException(e);
				}
			}
		}
	}
}
