/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.utils;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import java.time.Instant;
import java.time.LocalDateTime;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterByField;

public class FilterUtils
{
	/**
	 * Adds WHERE condition to a SELECT query
	 * @param filter condition definition
	 * @param column to check with filter. Usually it is {@code select.whereColumn(XXX)}
	 * @return updated SELECT query with new WHERE condition. Query to update is got from {@code column} parameter
	 */
	public static Select filterToWhere(FilterByField<?> filter, ColumnRelationBuilder<Select> column)
	{
		return filterToWhere(filter.getValue(), filter.getOperation(), column);
	}
	
	/**
	 * Adds WHERE condition to a SELECT query, putting a placeholder in place of condition value
	 * @param operation for comparison to use while filtering
	 * @param column to check with filter. Usually it is {@code select.whereColumn(XXX)}
	 * @return updated SELECT query with new WHERE condition and a placeholder in place of value. Query to update is got from {@code column} parameter
	 */
	public static Select filterToWhere(ComparisonOperation operation, ColumnRelationBuilder<Select> column)
	{
		BindMarker bm = QueryBuilder.bindMarker();
		switch (operation)
		{
		case LESS : return column.isLessThan(bm);
		case LESS_OR_EQUALS : return column.isLessThanOrEqualTo(bm);
		case GREATER : return column.isGreaterThan(bm);
		case GREATER_OR_EQUALS : return column.isGreaterThanOrEqualTo(bm);
		default : return column.isEqualTo(bm);
		}
	}
	
	/**
	 * Adds WHERE condition to a SELECT query
	 * @param value to use for filtering
	 * @param operation for comparison to use while filtering
	 * @param column to check with filter. Usually it is {@code select.whereColumn(XXX)}
	 * @return updated SELECT query with new WHERE condition. Query to update is got from {@code column} parameter
	 */
	public static Select filterToWhere(Object value, ComparisonOperation operation, ColumnRelationBuilder<Select> column)
	{
		Literal v = literal(value);
		switch (operation)
		{
		case LESS : return column.isLessThan(v);
		case LESS_OR_EQUALS : return column.isLessThanOrEqualTo(v);
		case GREATER : return column.isGreaterThan(v);
		case GREATER_OR_EQUALS : return column.isGreaterThanOrEqualTo(v);
		default : return column.isEqualTo(v);
		}
	}
	
	/**
	 * Adds WHERE condition for date and time columns to a SELECT query.
	 * Date and time columns to use for filtering are usually got by {@code select.whereColumn(XXX)}
	 * @param filter condition definition for timestamp to check
	 * @param select query to add conditions to
	 * @param dateColumn name of column that holds date part of timestamp
	 * @param timeColumn name of column that holds time part of timestamp
	 * @return updated SELECT query with new WHERE conditions
	 */
	public static Select timestampFilterToWhere(FilterByField<Instant> filter, Select select, String dateColumn, String timeColumn)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(filter.getValue(), CassandraCradleStorage.TIMEZONE_OFFSET);
		Select result = filterToWhere(ldt.toLocalDate(), filter.getOperation(), select.whereColumn(dateColumn));
		result = filterToWhere(ldt.toLocalTime(), filter.getOperation(), result.whereColumn(timeColumn));
		return result;
	}
	
	/**
	 * Binds timestamp to corresponding values of parameters in a prepared statement
	 * @param timestamp to bind to prepared statement
	 * @param builder to bind parameters in
	 * @param dateColumn name of column that holds date part of timestamp
	 * @param timeColumn name of column that holds time part of timestamp
	 * @return updated builder with parameters bound
	 */
	public static BoundStatementBuilder bindTimestamp(Instant timestamp, BoundStatementBuilder builder, String dateColumn, String timeColumn)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		builder = builder.setLocalDate(dateColumn, ldt.toLocalDate());
		builder = builder.setLocalTime(timeColumn, ldt.toLocalTime());
		return builder;
	}
}
