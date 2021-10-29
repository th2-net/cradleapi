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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.MultiColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterByField;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.utils.TimeUtils;

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
	 * @param bindMarkerName optional name for bind marker to bind parameter value in next calls
	 * @return updated SELECT query with new WHERE condition and a placeholder in place of value. Query to update is got from {@code column} parameter
	 */
	public static Select filterToWhere(ComparisonOperation operation, ColumnRelationBuilder<Select> column, String bindMarkerName)
	{
		BindMarker bm = bindMarkerName != null ? QueryBuilder.bindMarker(bindMarkerName) : QueryBuilder.bindMarker();
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
	 * @param operation for comparison to use while filtering
	 * @param select query to add conditions to
	 * @param dateColumn name of column that holds date part of timestamp
	 * @param timeColumn name of column that holds time part of timestamp
	 * @param dateMarker name of placeholder to use for date part of timestamp
	 * @param timeMarker name of placeholder to use for time part of timestamp
	 * @return updated SELECT query with new WHERE conditions
	 */
	public static Select timestampFilterToWhere(ComparisonOperation operation, Select select, String dateColumn, String timeColumn,
			String dateMarker, String timeMarker)
	{
		MultiColumnRelationBuilder<Select> mcrBuilder = select.whereColumns(dateColumn, timeColumn);
		switch (operation)
		{
			case LESS: return mcrBuilder.isLessThan(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
			case GREATER: return mcrBuilder.isGreaterThan(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
			case LESS_OR_EQUALS: return mcrBuilder.isLessThanOrEqualTo(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
			case GREATER_OR_EQUALS: return mcrBuilder.isGreaterThanOrEqualTo(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
			default: return mcrBuilder.isEqualTo(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
		}
	}
	
	/**
	 * Binds timestamp to corresponding values of parameters in a prepared statement
	 * @param timestamp to bind to prepared statement
	 * @param builder to bind parameters in
	 * @param dateMarker name of placeholder for date part of timestamp
	 * @param timeMarker name of placeholder for time part of timestamp
	 * @return updated builder with parameters bound
	 */
	public static BoundStatementBuilder bindTimestamp(Instant timestamp, BoundStatementBuilder builder, 
			String dateMarker, String timeMarker)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		builder = builder.setLocalDate(dateMarker, ldt.toLocalDate());
		builder = builder.setLocalTime(timeMarker, ldt.toLocalTime());
		return builder;
	}
	
	/**
	 * Converts given filter by Instant with "&gt;" or "&gt;=" operation into filter by LocalTime
	 * @param filterTimeFrom to convert
	 * @return filter with the same operation but using LocalTime
	 */
	public static FilterForGreater<LocalTime> filterTimeFrom(FilterForGreater<Instant> filterTimeFrom)
	{
		if (filterTimeFrom == null)
			return null;
		
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(filterTimeFrom.getValue());
		return filterTimeFrom.getOperation() == ComparisonOperation.GREATER
				? FilterForGreater.forGreater(ldt.toLocalTime())
				: FilterForGreater.forGreaterOrEquals(ldt.toLocalTime());
	}
	
	/**
	 * Converts given filter by Instant with "&lt;" or "&lt;=" operation into filter by LocalTime
	 * @param filterTimeTo to convert
	 * @return filter with the same operation but using LocalTime
	 */
	public static FilterForLess<LocalTime> filterTimeTo(FilterForLess<Instant> filterTimeTo)
	{
		if (filterTimeTo == null)
			return null;
		
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(filterTimeTo.getValue());
		return filterTimeTo.getOperation() == ComparisonOperation.LESS
				? FilterForLess.forLess(ldt.toLocalTime())
				: FilterForLess.forLessOrEquals(ldt.toLocalTime());
	}
	
	
	/**
	 * Finds page to start filtering from
	 * @param pageId specified in filter. Can be null
	 * @param timestampFrom specified in filter. Can be null
	 * @param book to filter data from
	 * @return page to start filtering from. If pageId is specified, it will be that page. 
	 * If timestampFrom is specified, it will be page that contains data for that timestamp.
	 * Else it will be the first page of the book
	 */
	public static PageInfo findFirstPage(PageId pageId, FilterForGreater<Instant> timestampFrom, BookInfo book)
	{
		if (pageId != null)
			return book.getPage(pageId);
		
		if (timestampFrom != null)
		{
			PageInfo page = book.findPage(timestampFrom.getValue());
			if (page != null)
				return page;
		}
		return book.getFirstPage();
	}
	
	/**
	 * Finds page to end filtering at
	 * @param pageId specified in filter. Can be null
	 * @param timestampTo specified in filter. Can be null
	 * @param book to filter data from
	 * @return page to end filtering at. If pageId is specified, it will be that page. 
	 * If timestampTo is specified, it will be the last page that contains data for that timestamp.
	 * Else it will be the last page of the book
	 */
	public static PageInfo findLastPage(PageId pageId, FilterForLess<Instant> timestampTo, BookInfo book)
	{
		if (pageId != null)
			return book.getPage(pageId);
		
		if (timestampTo != null)
		{
			PageInfo page = book.findPage(timestampTo.getValue());
			if (page != null)
				return page;
		}
		return book.getLastPage();
	}
}
