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

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.StorageConstants.PAGE;
import static com.exactpro.cradle.cassandra.StorageConstants.PART;
import static com.exactpro.cradle.cassandra.StorageConstants.SCOPE;
import static com.exactpro.cradle.cassandra.StorageConstants.START_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.START_TIME;

import java.time.LocalDate;
import java.time.LocalTime;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

public class CassandraTestEventFilter implements CassandraFilter<TestEventEntity>
{
	private static final String START_TIME_FROM = "startTimeFrom",
			START_TIME_TO = "startTimeTo";
	
	private final String page,
			scope,
			part;
	private final LocalDate startDate;
	private final FilterForGreater<LocalTime> startTimeFrom;
	private final FilterForLess<LocalTime> startTimeTo;
	
	public CassandraTestEventFilter(String page, LocalDate startDate, String scope, String part, 
			FilterForGreater<LocalTime> startTimeFrom, FilterForLess<LocalTime> startTimeTo)
	{
		this.page = page;
		this.startDate = startDate;
		this.scope = scope;
		this.part = part;
		this.startTimeFrom = startTimeFrom;
		this.startTimeTo = startTimeTo;
	}
	
	
	@Override
	public Select addConditions(Select select)
	{
		select = select.whereColumn(PAGE).isEqualTo(bindMarker())
				.whereColumn(START_DATE).isEqualTo(bindMarker())
				.whereColumn(SCOPE).isEqualTo(bindMarker())
				.whereColumn(PART).isEqualTo(bindMarker());
		
		if (startTimeFrom != null)
			select = FilterUtils.filterToWhere(startTimeFrom.getOperation(), select.whereColumn(START_TIME), START_TIME_FROM);
		if (startTimeTo != null)
			select = FilterUtils.filterToWhere(startTimeTo.getOperation(), select.whereColumn(START_TIME), START_TIME_TO);
		
		return select;
	}
	
	@Override
	public BoundStatementBuilder bindParameters(BoundStatementBuilder builder)
	{
		builder = builder.setString(PAGE, page)
				.setLocalDate(START_DATE, startDate)
				.setString(SCOPE, scope)
				.setString(PART, part);
		
		if (startTimeFrom != null)
			builder = builder.setLocalTime(START_TIME_FROM, startTimeFrom.getValue());
		if (startTimeTo != null)
			builder = builder.setLocalTime(START_TIME_TO, startTimeTo.getValue());
		
		return builder;
	}
	
	
	public String getPage()
	{
		return page;
	}
	
	public LocalDate getStartDate()
	{
		return startDate;
	}
	
	public String getScope()
	{
		return scope;
	}
	
	public String getPart()
	{
		return part;
	}
	
	public FilterForGreater<LocalTime> getStartTimeFrom()
	{
		return startTimeFrom;
	}
	
	public FilterForLess<LocalTime> getStartTimeTo()
	{
		return startTimeTo;
	}
}
