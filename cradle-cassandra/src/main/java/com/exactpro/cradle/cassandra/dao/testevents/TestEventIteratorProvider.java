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

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.cassandra.utils.TimestampBound;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.utils.TimeUtils;

public class TestEventIteratorProvider extends IteratorProvider<StoredTestEvent>
{
	/*
	 * The iterator creates CassandraTestEventFilter for each new DB request.
	 * Each request targets one partition, returning iterator to access the data.
	 * 
	 * Initial filter gets the starting page and adjusts left time bound to be not before the page start.
	 * If right time bound is within the same partition as the left time bound, 
	 * startTimestampTo is set for the filter, also meaning that this partition is the last one to query.
	 * Else startTimestampTo is not set and the partition is queried till the end.
	 * 
	 * If the queried partition was the last one for current page, 
	 * for the next request the page is changed and partition with the same name is queried.
	 * Else the next partition is queried.
	 * Left time bound is always not set.
	 * If right time bound is within this partition, 
	 * startTimestampTo is set, also meaning that this partition is the last one to query.
	 */
	
	private static final Logger logger = LoggerFactory.getLogger(TestEventIteratorProvider.class);
	
	private final TestEventOperator op;
	private final BookInfo book;
	private final TimestampBound startTimestampEndingBound;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private CassandraTestEventFilter cassandraFilter;
	
	public TestEventIteratorProvider(String requestInfo, TestEventFilter filter, BookOperators ops, BookInfo book,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		super(requestInfo);
		this.op = ops.getTestEventOperator();
		this.book = book;
		this.startTimestampEndingBound = FilterUtils.calcEndingTimeRightBound(filter.getPageId(), filter.getStartTimestampTo(), book);
		this.readAttrs = readAttrs;
		cassandraFilter = createInitialFilter(filter);
	}
	
	@Override
	public CompletableFuture<Iterator<StoredTestEvent>> nextIterator()
	{
		if (cassandraFilter == null)
			return CompletableFuture.completedFuture(null);
		
		logger.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, readAttrs)
				.thenApplyAsync(resultSet -> {
					PageId pageId = new PageId(book.getId(), cassandraFilter.getPage());
					cassandraFilter = createNextFilter(cassandraFilter);
					return new ConvertingPagedIterator<>(resultSet, entities -> {
						try
						{
							return EventEntityUtils.toStoredTestEvent(entities, pageId);
						}
						catch (Exception e)
						{
							throw new RuntimeException("Error while converting test event entity into Cradle test event", e);
						}
					});
				});
	}
	
	
	private CassandraTestEventFilter createInitialFilter(TestEventFilter filter)
	{
		PageInfo page = FilterUtils.findPage(filter.getPageId(), filter.getStartTimestampFrom(), book);
		
		TimestampBound dateTimeFromBound = FilterUtils.calcLeftTimeBound(filter.getStartTimestampFrom(), page.getStarted());
		
		LocalDate dateFrom = dateTimeFromBound.getTimestamp().toLocalDate();
		String part = dateTimeFromBound.getPart();
		FilterForGreater<LocalTime> timeFrom = dateTimeFromBound.toFilterForGreater();
		//If the whole query result fits one part by time, timeTo will be not null
		FilterForLess<LocalTime> timeTo = FilterUtils.calcCurrentTimeRightBound(dateFrom, part, page.getEnded(), startTimestampEndingBound);
		
		String parentId = getParentIdString(filter);
		return new CassandraTestEventFilter(page.getId().getName(), dateFrom, filter.getScope(), part, timeFrom, timeTo, parentId);
	}
	
	private CassandraTestEventFilter createNextFilter(CassandraTestEventFilter prevFilter)
	{
		if (prevFilter.getStartTimeTo() != null)  //Previous filter used the right bound for start timestamp, i.e. it was the very last query for this provider
			return null;
		
		LocalDate startDate = prevFilter.getStartDate();
		String part;
		
		PageInfo page = book.getPage(new PageId(book.getId(), prevFilter.getPage()));
		//Was queried partition the last one for current page?
		if (page.getEnded() == null || !CassandraTimeUtils.getPart(TimeUtils.toLocalTimestamp(page.getEnded())).equals(prevFilter.getPart()))
		{
			int partNumber = Integer.parseInt(prevFilter.getPart());
			if (partNumber >= 23)
			{
				startDate = startDate.plusDays(1);
				partNumber = 0;
			}
			else
				partNumber++;
			
			part = Integer.toString(partNumber);
		}
		else
		{
			page = book.getNextPage(page.getStarted());
			part = prevFilter.getPart();
		}
		
		FilterForLess<LocalTime> timeTo = FilterUtils.calcCurrentTimeRightBound(startDate, part, page.getEnded(), startTimestampEndingBound);
		return new CassandraTestEventFilter(page.getId().getName(), startDate, prevFilter.getScope(), part, null, timeTo, prevFilter.getParentId());
	}
	
	
	private String getParentIdString(TestEventFilter filter)
	{
		if (filter.isRoot())
			return "";
		
		StoredTestEventId parentId = filter.getParentId();
		if (parentId != null)
			return parentId.toString();
		
		return null;
	}
}
