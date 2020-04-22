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

import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.filters.FilterByField;

public class FilterUtils
{
	public static Select filterToWhere(FilterByField<?> filter, ColumnRelationBuilder<Select> column)
	{
		Literal v = literal(filter.getValue());
		switch (filter.getOperation())
		{
		case LESS : return column.isLessThan(v);
		case LESS_OR_EQUALS : return column.isLessThanOrEqualTo(v);
		case GREATER : return column.isGreaterThan(v);
		case GREATER_OR_EQUALS : return column.isGreaterThanOrEqualTo(v);
		case NOT_EQUALS : return column.isNotEqualTo(v);
		default : return column.isEqualTo(v);
		}
	}
}
