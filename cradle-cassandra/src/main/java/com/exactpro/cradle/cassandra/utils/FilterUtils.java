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
	public static Select filterToWhere(FilterByField<?> filter, ColumnRelationBuilder<Select> column)
	{
		return filterToWhere(filter.getValue(), filter.getOperation(), column);
	}
	
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
	
	public static Select timestampFilterToWhere(FilterByField<Instant> filter, 
			ColumnRelationBuilder<Select> dateColumn, ColumnRelationBuilder<Select> timeColumn)
	{
		LocalDateTime ldt = LocalDateTime.ofInstant(filter.getValue(), CassandraCradleStorage.TIMEZONE);
		Select result = filterToWhere(ldt.toLocalDate(), filter.getOperation(), dateColumn);
		result = filterToWhere(ldt.toLocalTime(), filter.getOperation(), timeColumn);
		return result;
	}
}
