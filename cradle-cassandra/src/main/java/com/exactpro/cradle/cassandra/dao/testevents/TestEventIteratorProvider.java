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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
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
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.utils.TimeUtils;

public class TestEventIteratorProvider extends IteratorProvider<StoredTestEvent>
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventIteratorProvider.class);
	
	private final TestEventOperator op;
	private final BookInfo book;
	private final TimestampBound startTimestampBound;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private CassandraTestEventFilter cassandraFilter;
	
	public TestEventIteratorProvider(String requestInfo, TestEventFilter filter, BookOperators ops, BookInfo book,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		super(requestInfo);
		this.op = ops.getTestEventOperator();
		this.book = book;
		this.startTimestampBound = calcStartTimestampRightBound(filter);
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
					return new ConvertingPagedIterator<>(resultSet, entity -> {
						try
						{
							return EventEntityUtils.toStoredTestEvent(Collections.singleton(entity), pageId);
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
		PageInfo page = findPage(filter);
		LocalDateTime dateTimeFrom = filter.getStartTimestampFrom() != null 
				? TimeUtils.toLocalTimestamp(filter.getStartTimestampFrom().getValue()) 
				: TimeUtils.toLocalTimestamp(page.getStarted());
		LocalDate date = dateTimeFrom.toLocalDate();
		String part = CassandraTimeUtils.getPart(dateTimeFrom);
		FilterForGreater<LocalTime> timeFrom = filter.getStartTimestampFrom() != null ? FilterUtils.filterTimeFrom(filter.getStartTimestampFrom()) : null;
		FilterForLess<LocalTime> timeTo = getStartTimestampRightBound(date, part);
		return new CassandraTestEventFilter(page.getId().getName(), date, filter.getScope(), part, timeFrom, timeTo);
	}
	
	private CassandraTestEventFilter createNextFilter(CassandraTestEventFilter prevFilter)
	{
		if (prevFilter.getStartTimeTo() != null)  //Previous filter used the right bound for start timestamp, i.e. it was the very last query for this provider
			return null;
		
		LocalDate startDate = prevFilter.getStartDate();
		int partNumber = Integer.parseInt(prevFilter.getPart());
		if (partNumber >= 23)
		{
			startDate = startDate.plusDays(1);
			partNumber = 0;
		}
		else
			partNumber++;
		
		String part = Integer.toString(partNumber);
		FilterForLess<LocalTime> timeTo = getStartTimestampRightBound(startDate, part);
		return new CassandraTestEventFilter(prevFilter.getPage(), startDate, prevFilter.getScope(), part, null, timeTo);
	}
	
	
	private PageInfo findPage(TestEventFilter filter)
	{
		return filter.getPageId() != null ? book.getPage(filter.getPageId()) : book.getFirstPage();
	}
	
	private TimestampBound calcStartTimestampRightBound(TestEventFilter filter)
	{
		FilterForLess<Instant> timestampTo = filter.getStartTimestampTo();
		if (timestampTo != null)
			return new TimestampBound(TimeUtils.toLocalTimestamp(timestampTo.getValue()), timestampTo.getOperation(), true);
		
		//FIXME: in both cases below to use timestamp of last event stored in page or book, not now(). This is because event can have start_date after now()
		
		if (filter.getPageId() != null)
		{
			PageInfo page = book.getPage(filter.getPageId());
			LocalDateTime timestamp = page.isActive() ? LocalDateTime.now() : TimeUtils.toLocalTimestamp(page.getEnded());
			return new TimestampBound(timestamp, ComparisonOperation.LESS_OR_EQUALS, false);  //Page may have ended after the official end, so we'll query till actual end
		}
		
		return new TimestampBound(LocalDateTime.now(), ComparisonOperation.LESS_OR_EQUALS, true);
	}
	
	private FilterForLess<LocalTime> getStartTimestampRightBound(LocalDate date, String part)
	{
		LocalDate lastDate = startTimestampBound.getTimestamp().toLocalDate();
		if (date.equals(lastDate) && part.equals(startTimestampBound.getPart()))
		{
			LocalTime time = startTimestampBound.isUseTime() ? startTimestampBound.getTimestamp().toLocalTime() : LocalTime.of(23, 59, 59, 999999);
			return startTimestampBound.getOperation() == ComparisonOperation.LESS 
					? FilterForLess.forLess(time)
					: FilterForLess.forLessOrEquals(time);
		}
		if (date.isAfter(lastDate))
			return FilterForLess.forLessOrEquals(LocalTime.of(23, 59, 59, 999999));
		return null;  //Will query till the end of part
	}
}
