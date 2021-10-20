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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;

public class TestEventIteratorProvider extends IteratorProvider<StoredTestEvent>
{
	/*
	 * The iterator creates CassandraTestEventFilter for each new DB query.
	 * Each query targets one page, returning iterator to access the data.
	 * 
	 * Initial filter gets the starting page and the ending page. 
	 * If the ending page was queried, no more queries will be done, meaning the end of data
	 */
	
	private static final Logger logger = LoggerFactory.getLogger(TestEventIteratorProvider.class);
	
	private final TestEventOperator op;
	private final BookInfo book;
	private final PageInfo firstPage, lastPage;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private final int limit;
	private final AtomicInteger returned;
	private CassandraTestEventFilter cassandraFilter;
	
	public TestEventIteratorProvider(String requestInfo, TestEventFilter filter, BookOperators ops, BookInfo book,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		super(requestInfo);
		this.op = ops.getTestEventOperator();
		this.book = book;
		this.firstPage = FilterUtils.findFirstPage(filter.getPageId(), filter.getStartTimestampFrom(), book);
		this.lastPage = FilterUtils.findLastPage(filter.getPageId(), filter.getStartTimestampTo(), book);
		this.readAttrs = readAttrs;
		this.limit = filter.getLimit();
		this.returned = new AtomicInteger();
		cassandraFilter = createInitialFilter(filter);
	}
	
	@Override
	public CompletableFuture<Iterator<StoredTestEvent>> nextIterator()
	{
		if (cassandraFilter == null)
			return CompletableFuture.completedFuture(null);
		if (limit > 0 && returned.get() >= limit)
		{
			logger.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return CompletableFuture.completedFuture(null);
		}
		
		logger.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, readAttrs)
				.thenApplyAsync(resultSet -> {
					PageId pageId = new PageId(book.getId(), cassandraFilter.getPage());
					cassandraFilter = createNextFilter(cassandraFilter);
					return new ConvertingPagedIterator<>(resultSet, limit, returned, entities -> {
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
		String parentId = getParentIdString(filter);
		return new CassandraTestEventFilter(firstPage.getId().getName(), filter.getScope(), 
				filter.getStartTimestampFrom(), filter.getStartTimestampTo(), parentId);
	}
	
	private CassandraTestEventFilter createNextFilter(CassandraTestEventFilter prevFilter)
	{
		PageInfo prevPage = book.getPage(new PageId(book.getId(), prevFilter.getPage()));
		if (prevPage == lastPage)
			return null;
		
		PageInfo nextPage = book.getNextPage(prevPage.getStarted());
		return new CassandraTestEventFilter(nextPage.getId().getName(), prevFilter.getScope(), 
				prevFilter.getStartTimestampFrom(), prevFilter.getStartTimestampTo(), prevFilter.getParentId());
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
