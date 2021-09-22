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
import static com.exactpro.cradle.cassandra.StorageConstants.PARENT_ID;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

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
	private final String parentId;
	
	public CassandraTestEventFilter(String page, String scope, String part, 
			LocalDate startDate, 
			FilterForGreater<LocalTime> startTimeFrom, FilterForLess<LocalTime> startTimeTo,
			String parentId)
	{
		this.page = page;
		this.scope = scope;
		this.part = part;
		this.startDate = startDate;
		this.startTimeFrom = startTimeFrom;
		this.startTimeTo = startTimeTo;
		this.parentId = parentId;
	}
	
	
	@Override
	public Select addConditions(Select select)
	{
		select = select.whereColumn(PAGE).isEqualTo(bindMarker())
				.whereColumn(SCOPE).isEqualTo(bindMarker())
				.whereColumn(PART).isEqualTo(bindMarker());
		
		if (startDate != null)
			select = select.whereColumn(START_DATE).isEqualTo(bindMarker());
		
		if (startTimeFrom != null)
			select = FilterUtils.filterToWhere(startTimeFrom.getOperation(), select.whereColumn(START_TIME), START_TIME_FROM);
		if (startTimeTo != null)
			select = FilterUtils.filterToWhere(startTimeTo.getOperation(), select.whereColumn(START_TIME), START_TIME_TO);
		if (parentId != null)
			select = select.whereColumn(PARENT_ID).isEqualTo(bindMarker());
		
		return select;
	}
	
	@Override
	public BoundStatementBuilder bindParameters(BoundStatementBuilder builder)
	{
		builder = builder.setString(PAGE, page)
				.setString(SCOPE, scope)
				.setString(PART, part);
		
		if (startDate != null)
			builder = builder.setLocalDate(START_DATE, startDate);
		
		if (startTimeFrom != null)
			builder = builder.setLocalTime(START_TIME_FROM, startTimeFrom.getValue());
		if (startTimeTo != null)
			builder = builder.setLocalTime(START_TIME_TO, startTimeTo.getValue());
		if (parentId != null)
			builder = builder.setString(PARENT_ID, parentId);
		
		return builder;
	}
	
	
	public String getPage()
	{
		return page;
	}
	
	public String getScope()
	{
		return scope;
	}
	
	public String getPart()
	{
		return part;
	}
	
	public LocalDate getStartDate()
	{
		return startDate;
	}
	
	public FilterForGreater<LocalTime> getStartTimeFrom()
	{
		return startTimeFrom;
	}
	
	public FilterForLess<LocalTime> getStartTimeTo()
	{
		return startTimeTo;
	}
	
	public String getParentId()
	{
		return parentId;
	}
	
	
	@Override
	public String toString()
	{
		List<String> result = new ArrayList<>(10);
		if (page != null)
			result.add("page=" + page);
		if (scope != null)
			result.add("scope=" + scope);
		if (part != null)
			result.add("part=" + part);
		if (startDate != null)
			result.add("start date="+startDate);
		if (startTimeFrom != null)
			result.add("timestamp" + startTimeFrom);
		if (startTimeTo != null)
			result.add("timestamp" + startTimeTo);
		if (parentId != null)
			result.add("parentId=" + parentId);
		return String.join(", ", result);
	}
}
