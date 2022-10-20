/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.EventBatchDurationCache;
import com.exactpro.cradle.cassandra.EventBatchDurationWorker;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;
import com.exactpro.cradle.cassandra.iterators.SkippingConvertingPagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.workers.EventsWorker.mapTestEventEntity;

public class TestEventIteratorProvider extends IteratorProvider<StoredTestEvent> {
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
	private final ExecutorService composingService;
	private final SelectQueryExecutor selectQueryExecutor;
	private final TestEventEntityConverter entityConverter;
	private final PageInfo firstPage, lastPage;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private final int limit;
	private final AtomicInteger returned;
	private CassandraTestEventFilter cassandraFilter;
	private final EventBatchDurationWorker eventBatchDurationWorker;
	private final Instant actualFrom;
	
	public TestEventIteratorProvider(String requestInfo, TestEventFilter filter, CassandraOperators operators, BookInfo book,
									 ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
									 EventBatchDurationWorker eventBatchDurationWorker,
									 Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
									 Instant actualFrom) {
		super(requestInfo);
		this.op = operators.getTestEventOperator();
		this.entityConverter = operators.getTestEventEntityConverter();
		this.book = book;
		this.composingService = composingService;
		this.selectQueryExecutor = selectQueryExecutor;

		// setup first & last pages according to filter order
		PageInfo pageFrom = FilterUtils.findFirstPage(filter.getPageId(), effectiveStartTimestampFrom(filter), book);
		PageInfo pageTo = FilterUtils.findLastPage(filter.getPageId(), effectiveStartTimestampTo(filter), book);
		Order order = filter.getOrder();
		this.firstPage = (order == Order.DIRECT) ? pageFrom : pageTo;
		this.lastPage = (order == Order.DIRECT) ? pageTo : pageFrom;

		this.eventBatchDurationWorker = eventBatchDurationWorker;
		this.actualFrom = actualFrom;

		this.readAttrs = readAttrs;
		this.limit = filter.getLimit();
		this.returned = new AtomicInteger();
		this.cassandraFilter = createInitialFilter(filter);
	}


	private FilterForGreater<Instant> effectiveStartTimestampFrom(TestEventFilter filter) {
		if (filter.getOrder() == Order.DIRECT && filter.getId() != null) {
			FilterForGreater<Instant> result = new FilterForGreater<>(filter.getId().getStartTimestamp());
			result.setGreaterOrEquals();
			return result;
		}
		return filter.getStartTimestampFrom();
	}


	private FilterForLess<Instant> effectiveStartTimestampTo(TestEventFilter filter) {
		if (filter.getOrder() == Order.REVERSE && filter.getId() != null) {
			FilterForLess<Instant> result = new FilterForLess<>(filter.getId().getStartTimestamp());
			result.setLessOrEquals();
			return result;
		}
		return filter.getStartTimestampTo();
	}

	@Override
	public CompletableFuture<Iterator<StoredTestEvent>> nextIterator() {

		if (cassandraFilter == null)
			return CompletableFuture.completedFuture(null);

		if (limit > 0 && returned.get() >= limit) {
			logger.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return CompletableFuture.completedFuture(null);
		}
		
		logger.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, selectQueryExecutor, getRequestInfo(), readAttrs)
				.thenApplyAsync(resultSet -> {
					PageId pageId = new PageId(book.getId(), cassandraFilter.getPage());
					cassandraFilter = createNextFilter(cassandraFilter);

					return new SkippingConvertingPagedIterator<>(
							resultSet,
							selectQueryExecutor,
							limit,
							returned,
							entity -> mapTestEventEntity(pageId, entity),
							entityConverter::getEntity,
							// This skip function checks if batch interval crosses requested filter interval
							convertedEntity -> convertedEntity.getLastStartTimestamp().isBefore(actualFrom),
							getRequestInfo());
				}, composingService);
	}


	private CassandraTestEventFilter createInitialFilter(TestEventFilter filter) {
		/*
			Only initial filter needs to be adjusted with max duration,
			since `createNextFilter` just passes timestamps from previous to next filters
		 */
		long duration = eventBatchDurationWorker.getMaxDuration(new EventBatchDurationCache.CacheKey(filter.getBookId().getName(), firstPage.getId().getName(), filter.getScope()), readAttrs);
		FilterForGreater<Instant> newFrom = FilterForGreater.forGreater(actualFrom.minusMillis(duration));

		String parentId = getParentIdString(filter);
		return new CassandraTestEventFilter(
				book.getId().getName(),
				firstPage.getId().getName(),
				filter.getScope(),
				newFrom,
				filter.getStartTimestampTo(),
				filter.getId(),
				parentId,
				filter.getOrder());
	}
	
	private CassandraTestEventFilter createNextFilter(CassandraTestEventFilter prevFilter) {

		PageInfo prevPage = book.getPage(new PageId(book.getId(), prevFilter.getPage()));
		if (prevPage == lastPage)
			return null;

		// calculate next page according to filter order
		PageInfo nextPage;
		if (prevFilter.getOrder() == Order.DIRECT)
			nextPage = book.getNextPage(prevPage.getStarted());
		else
			nextPage = book.getPreviousPage(prevPage.getStarted());

		return new CassandraTestEventFilter(
				book.getId().getName(),
				nextPage.getId().getName(),
				prevFilter.getScope(),
				prevFilter.getStartTimestampFrom(),
				prevFilter.getStartTimestampTo(),
				prevFilter.getId(),
				prevFilter.getParentId(),
				prevFilter.getOrder());
	}
	
	
	private String getParentIdString(TestEventFilter filter) {

		if (filter.isRoot())
			return "";
		
		StoredTestEventId parentId = filter.getParentId();
		if (parentId != null)
			return parentId.toString();
		
		return null;
	}
}
