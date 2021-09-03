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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
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
	 * Calculates left time bound based on value specified in filter and actually available minimum
	 * @param filterFrom left time bound specified in filter
	 * @param minimumFrom actually available minimum
	 * @return left bound to be used
	 */
	public static TimestampBound calcLeftTimeBound(FilterForGreater<Instant> filterFrom, Instant minimumFrom)
	{
		if (filterFrom == null || filterFrom.getValue().isBefore(minimumFrom))
			return new TimestampBound(minimumFrom, ComparisonOperation.GREATER_OR_EQUALS);
		return new TimestampBound(filterFrom.getValue(), filterFrom.getOperation());
	}
	
	/**
	 * Calculates right time bound based on value specified in filter and actually available maximum
	 * @param filterTo right time bound specified in filter
	 * @param maximumTo actually available maximum
	 * @return right bound to be used
	 */
	public static TimestampBound calcRightTimeBound(FilterForLess<Instant> filterTo, Instant maximumTo)
	{
		if (filterTo == null || filterTo.getValue().isAfter(maximumTo))
			return new TimestampBound(maximumTo, ComparisonOperation.LESS_OR_EQUALS);
		return new TimestampBound(filterTo.getValue(), filterTo.getOperation());
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
	public static PageInfo findPage(PageId pageId, FilterForGreater<Instant> timestampFrom, BookInfo book)
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
	 * Calculates right time bound when filtering ends
	 * @param pageId specified in filter. Can be null
	 * @param timestampTo specified in filter. Can be null
	 * @param book to filter data from
	 * @return right time bound to be used as condition to end filtering
	 */
	public static TimestampBound calcEndingTimeRightBound(PageId pageId, FilterForLess<Instant> timestampTo, BookInfo book)
	{
		if (pageId != null)
		{
			PageInfo page = book.getPage(pageId);
			return calcRightTimeBound(timestampTo,
					page.isActive() ? Instant.now() : page.getEnded());
		}
		
		return calcRightTimeBound(timestampTo, Instant.now());
	}
	
	/**
	 * Calculates filter condition for right time bound for current filtering request
	 * @param date used in current filtering request
	 * @param part part used in current filtering request
	 * @param pageEnd ending timestamp for page used in current filtering request
	 * @param endingBound right time bound to end filtering, previously calculated with {@link #calcEndingTimeRightBound(PageId, FilterForLess, BookInfo)}
	 * @return filter condition for right time bound to be used in current filtering request. 
	 * If endingBound is beyond pageEnd, filter condition will be null meaning that filtering will be done till partition end and 
	 * additional filtering request should be generated after that
	 */
	public static FilterForLess<LocalTime> calcCurrentTimeRightBound(LocalDate date, String part, Instant pageEnd, TimestampBound endingBound)
	{
		if (pageEnd != null)
		{
			LocalDateTime pageEndDateTime = TimeUtils.toLocalTimestamp(pageEnd);
			if (endingBound.getTimestamp().isAfter(pageEndDateTime))
				return null;  //Not setting the right bound to query till the end of last partition of the page. Next page will be queried using the same partition
		}
		
		LocalDate lastDate = endingBound.getTimestamp().toLocalDate();
		if (date.equals(lastDate) && part.equals(endingBound.getPart()))
			return endingBound.toFilterForLess();
		if (date.isAfter(lastDate))
			return FilterForLess.forLessOrEquals(LocalTime.of(23, 59, 59, 999999));  //For case of wrong right bound calculation
		return null;  //Will query till the end of part
	}
}
