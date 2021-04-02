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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.CassandraTablesCreator;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
	public static final int ATTEMPTS_TO_CHECK_AVAILABILITY = 10;
	public static final int CHECK_AVABILITY_TIMEOUT_MS = 5000;

	private String currentKeyspace;
	
	private final String keyspaceQuery = QueryBuilder.selectFrom(AMAZON_SYSTEM_KEYSPACE, KEYSPACES_TABLE)
			.all()
			.whereColumn(wrap(KEYSPACES_COLUMN)).isEqualTo(QueryBuilder.bindMarker())
			.limit(1)
			.asCql();

	private final String tableStatusQuery = QueryBuilder.selectFrom(AMAZON_SYSTEM_KEYSPACE, TABLES_TABLE)
			.all()
			.whereColumn(wrap(KEYSPACES_COLUMN)).isEqualTo(QueryBuilder.bindMarker())
			.whereColumn(wrap(TABLE_NAME_COLUMN)).isEqualTo(QueryBuilder.bindMarker())
			.limit(1)
			.asCql();


	private static String wrap(String arg)
	{
		return StringUtils.wrap(arg, '"');
	}
	
	public AmazonTablesCreator(QueryExecutor exec, CassandraStorageSettings settings)
	{
		super(exec, settings);
	}

	@Override
	protected boolean isKeyspaceExists(String keyspace) throws IOException
	{
		return false;
	}

	@Override
	protected boolean isTableExists(String tableName) throws IOException
	{
		return false;
	}

	@Override
	protected void doAfterKeyspaceCreating(String keyspace)
	{
		currentKeyspace = keyspace;
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
		checkKeyspaceAvailability();
	}

	private void checkKeyspaceAvailability() throws IOException
	{
		boolean currentStatus;
		for (int i = 0; i < ATTEMPTS_TO_CHECK_AVAILABILITY; i++)
		{
			if (currentStatus = getKeyspaceStatus(currentKeyspace))
				return;
			if (i == ATTEMPTS_TO_CHECK_AVAILABILITY - 1)
			{
				logger.debug("Last status of keyspace {} is {}", currentKeyspace, currentStatus);
				throw new IOException("Keyspace " + currentKeyspace + " is not available");
			}
			try
			{
				Thread.sleep(CHECK_AVABILITY_TIMEOUT_MS);
			}
			catch (InterruptedException e)
			{
				throw new IOException(e);
			}
		}
	}

	protected boolean getKeyspaceStatus(String keyspace) throws IOException
	{
		ResultSet rs = exec.executeQuery(keyspaceQuery, false, keyspace);
		Row row = rs.one();

		return row != null;
	}

	@Override
	public void createAll() throws IOException
	{
		super.createAll();
		checkTablesAvailability(Arrays.asList(
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

	private void checkTablesAvailability(Collection<String> tablesNames) throws IOException
	{
		for (String tableName : tablesNames)
		{
			String currentStatus;
			for (int i = 0; i < ATTEMPTS_TO_CHECK_AVAILABILITY; i++)
			{
				if (ACTIVE_STATUS.equals(currentStatus = getTableStatus(tableName)))
					break;
				if (i == ATTEMPTS_TO_CHECK_AVAILABILITY - 1)
				{
					logger.warn("Last status of table '{}' is '{}'", tableName, currentStatus);
					throw new IOException("Table '" + tableName + "' is not available");
				}
				try
				{
					Thread.sleep(CHECK_AVABILITY_TIMEOUT_MS);
				}
				catch (InterruptedException e)
				{
					throw new IOException(e);
				}
			}
		}
	}

	//	@Override
//	protected boolean isColumnExists(String tableName, String columnName) throws IOException
//	{
//		Select select = columnChecksMapping.computeIfAbsent(tableName, this::generateColumnCheckSelect);
//		ResultSet rs = exec.executeQuery(select.build(), false);
//		return rs.getColumnDefinitions().contains(columnName);
//	}

//	private Select generateColumnCheckSelect(String tableName)
//	{
//		return QueryBuilder.selectFrom(currentKeyspace, tableName)
//				.all()
//				.limit(0);
//	}

//	private boolean isResultNotEmpty(SimpleStatement statement) throws IOException
//	{
//		ResultSet rs = exec.executeQuery(statement, false);
//		return rs.one() != null; 
//	}
}
