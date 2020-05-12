/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.utils;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import java.time.Instant;
import java.time.LocalDateTime;

import com.datastax.oss.driver.api.querybuilder.Literal;
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
		case NOT_EQUALS : return column.isNotEqualTo(v);
		default : return column.isEqualTo(v);
		}
	}
	
	/**
	 * Adds WHERE condition for date and time columns to a SELECT query.
	 * Date and time columns to use for filtering are usually got by {@code select.whereColumn(XXX)}
	 * @param filter condition definition for timestamp to check
	 * @param dateColumn that holds date part of timestamp
	 * @param timeColumn that holds time part of timestamp
	 * @return updated SELECT query with new WHERE conditions
	 */
	public static Select timestampFilterToWhere(FilterByField<Instant> filter, 
			ColumnRelationBuilder<Select> dateColumn, ColumnRelationBuilder<Select> timeColumn)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(filter.getValue(), CassandraCradleStorage.TIMEZONE_OFFSET);
		Select result = filterToWhere(ldt.toLocalDate(), filter.getOperation(), dateColumn);
		result = filterToWhere(ldt.toLocalTime(), filter.getOperation(), timeColumn);
		return result;
	}
}
