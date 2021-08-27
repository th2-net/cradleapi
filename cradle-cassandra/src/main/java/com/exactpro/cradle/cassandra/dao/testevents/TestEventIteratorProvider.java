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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.utils.TimeUtils;

public class TestEventIteratorProvider extends IteratorProvider<StoredTestEvent>
{
	private final TestEventFilter filter;
	private final TestEventOperator op;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private CassandraTestEventFilter cassandraFilter;
	
	public TestEventIteratorProvider(String requestInfo, TestEventFilter filter, BookOperators ops, 
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		super(requestInfo);
		this.filter = new TestEventFilter(filter);
		this.op = ops.getTestEventOperator();
		this.readAttrs = readAttrs;
		cassandraFilter = createFilter(filter, null);
	}
	
	@Override
	public CompletableFuture<Iterator<StoredTestEvent>> nextIterator()
	{
		if (cassandraFilter == null)
			return CompletableFuture.completedFuture(null);
		
		return op.getByFilter(cassandraFilter, readAttrs)
				.thenApplyAsync(resultSet -> {
					cassandraFilter = createFilter(filter, cassandraFilter);
					return new ConvertingPagedIterator<>(resultSet, entity -> {
						try
						{
							return EventEntityUtils.toStoredTestEvent(Collections.singleton(entity), filter.getPageId());
						}
						catch (Exception e)
						{
							throw new RuntimeException("Error while converting test event entity into Cradle test event", e);
						}
					});
				});
	}
	
	
	private CassandraTestEventFilter createFilter(TestEventFilter generalFilter, CassandraTestEventFilter prevFilter)
	{
		//TODO: implement creation of filter next to previous one. pageId is optional
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(generalFilter.getStartTimestampFrom().getValue());
		FilterForGreater<LocalTime> timeFrom = createFilterTimeFrom(generalFilter.getStartTimestampFrom());
		FilterForLess<LocalTime> timeTo = createFilterTimeTo(generalFilter.getStartTimestampTo());
		return new CassandraTestEventFilter(generalFilter.getPageId().getName(), 
				ldt.toLocalDate(), 
				generalFilter.getScope(), 
				CassandraTimeUtils.getPart(ldt), 
				timeFrom, timeTo);
	}
	
	private FilterForGreater<LocalTime> createFilterTimeFrom(FilterForGreater<Instant> filterTimeFrom)
	{
		if (filterTimeFrom == null)
			return null;
		
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(filterTimeFrom.getValue());
		FilterForGreater<LocalTime> result = new FilterForGreater<>(ldt.toLocalTime());
		if (filterTimeFrom.getOperation() == ComparisonOperation.GREATER)
			result.setGreater();
		else
			result.setGreaterOrEquals();
		return result;
	}
	
	private FilterForLess<LocalTime> createFilterTimeTo(FilterForLess<Instant> filterTimeTo)
	{
		if (filterTimeTo == null)
			return null;
		
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(filterTimeTo.getValue());
		FilterForLess<LocalTime> result = new FilterForLess<>(ldt.toLocalTime());
		if (filterTimeTo.getOperation() == ComparisonOperation.LESS)
			result.setLess();
		else
			result.setLessOrEquals();
		return result;
	}
}
