/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.EventBatchDurationWorker;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.iterators.ConvertingIterator;
import com.exactpro.cradle.iterators.FilteringIterator;
import com.exactpro.cradle.iterators.LimitedIterator;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.utils.FilterUtils.findFirstPage;
import static com.exactpro.cradle.cassandra.utils.FilterUtils.findFirstTimestamp;
import static com.exactpro.cradle.cassandra.utils.FilterUtils.findLastTimestamp;
import static java.lang.Math.max;

public class TestEventIteratorProvider extends IteratorProvider<StoredTestEvent> {
	/*
	 * The iterator creates CassandraTestEventFilter for each new DB query.
	 * Each query targets one page, returning iterator to access the data.
	 *
	 * Initial filter gets the starting page and the ending page.
	 * If the ending page was queried, no more queries will be done, meaning the end of data
	 */

	private static final Logger LOGGER = LoggerFactory.getLogger(TestEventIteratorProvider.class);

	private final TestEventOperator op;
    private final ExecutorService composingService;
	private final SelectQueryExecutor selectQueryExecutor;
	private final TestEventEntityConverter entityConverter;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	/** limit must be strictly positive ( limit greater than 0 ) */
	private final int limit;
	private final AtomicInteger returned = new AtomicInteger();
    private final Instant actualFrom;
	private final Iterator<PageInfo> pageProvider;
	private final TestEventFilter filter;
	private final FilterForGreater<Instant> leftBoundFilter;
	private final TestEventEntityToStoredMapper mapper;

	public TestEventIteratorProvider(String requestInfo, TestEventFilter filter, CassandraOperators operators, BookInfo book,
									 ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
									 EventBatchDurationWorker eventBatchDurationWorker,
									 Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
									 Instant actualFrom,
									 TestEventEntityToStoredMapper mapper) {
		super(requestInfo);
		this.op = operators.getTestEventOperator();
		this.entityConverter = operators.getTestEventEntityConverter();
        this.composingService = composingService;
		this.selectQueryExecutor = selectQueryExecutor;
		this.actualFrom = actualFrom;
		this.readAttrs = readAttrs;
		this.limit = filter.getLimit();
		this.leftBoundFilter = createLeftBoundFilter(book, eventBatchDurationWorker, actualFrom, readAttrs, filter);
		this.filter = filter;
		this.mapper = mapper;

		this.pageProvider = book.getPages(
			// we must calculate the first timestamp by origin filter because
			// left bound filter is calculated according to max duration of event batch on the first page
			findFirstTimestamp(filter.getPageId(), filter.getStartTimestampFrom(), book),
			findLastTimestamp(filter.getPageId(), filter.getStartTimestampTo(), book),
			filter.getOrder()
		);
	}

	@Override
	public CompletableFuture<Iterator<StoredTestEvent>> nextIterator() {
		if (!pageProvider.hasNext()) {
			return CompletableFuture.completedFuture(null);
		}
		PageInfo nextPage = pageProvider.next();

		if (limit > 0 && returned.get() >= limit) {
			LOGGER.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return CompletableFuture.completedFuture(null);
		}

		CassandraTestEventFilter cassandraFilter = createFilter(nextPage, max(limit - returned.get(), 0));
		
		LOGGER.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, selectQueryExecutor, getRequestInfo(), readAttrs)
				.thenApplyAsync(resultSet -> {
					PagedIterator<TestEventEntity> pagedIterator = new PagedIterator<>(
							resultSet,
							selectQueryExecutor,
							entityConverter::getEntity,
							getRequestInfo());
					ConvertingIterator<TestEventEntity, StoredTestEvent> convertingIterator = new ConvertingIterator<>(
							pagedIterator, entity ->
							mapper.convert(entity, nextPage.getId()));
					FilteringIterator<StoredTestEvent> filteringIterator = new FilteringIterator<>(
							convertingIterator,
							convertedEntity -> !convertedEntity.getLastStartTimestamp().isBefore(actualFrom));


					return limit > 0 ? new LimitedIterator<>(filteringIterator, limit) : filteringIterator;
				}, composingService);
	}

	private CassandraTestEventFilter createFilter(@Nonnull PageInfo pageInfo, int updatedLimit) {
		return new CassandraTestEventFilter(
				pageInfo.getId(),
				filter.getScope(),
				leftBoundFilter,
				filter.getStartTimestampTo(),
				filter.getId(),
				getParentIdString(filter),
				updatedLimit,
				filter.getOrder());
	}

	private String getParentIdString(TestEventFilter filter) {

		if (filter.isRoot())
			return "";

		StoredTestEventId parentId = filter.getParentId();
		if (parentId != null)
			return parentId.toString();

		return null;
	}

	private static FilterForGreater<Instant> createLeftBoundFilter(BookInfo bookInfo, EventBatchDurationWorker eventBatchDurationWorker, Instant actualFrom, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs, TestEventFilter filter) {
		/*
			Only initial filter needs to be adjusted with max duration,
			since `createNextFilter` just passes timestamps from previous to next filters
		 */
		PageInfo firstPage = findFirstPage(filter.getPageId(), filter.getStartTimestampFrom(), bookInfo);
		long duration = 0;
		if (firstPage != null) {
			duration = eventBatchDurationWorker.getMaxDuration(filter.getBookId().getName(), firstPage.getName(), filter.getScope(), readAttrs);
		}

		ComparisonOperation operation = filter.getStartTimestampFrom() == null ? ComparisonOperation.GREATER : filter.getStartTimestampFrom().getOperation();
		if (operation.equals(ComparisonOperation.GREATER)) {
			return FilterForGreater.forGreater(actualFrom.minusMillis(duration));
		} else {
			return FilterForGreater.forGreaterOrEquals(actualFrom.minusMillis(duration));
		}
	}
}
