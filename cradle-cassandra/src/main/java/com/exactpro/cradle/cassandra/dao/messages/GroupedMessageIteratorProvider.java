/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.messages.converters.GroupedMessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.cassandra.workers.MessagesWorker;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.iterators.ConvertingIterator;
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.FIELD_FIRST_MESSAGE_TIME;

public class GroupedMessageIteratorProvider extends IteratorProvider<StoredGroupedMessageBatch>
{
	public static final Logger logger = LoggerFactory.getLogger(GroupedMessageIteratorProvider.class);

	private final GroupedMessageBatchOperator op;
	private final GroupedMessageBatchEntityConverter converter;
	private final BookInfo book;
	private final ExecutorService composingService;
	private final SelectQueryExecutor selectQueryExecutor;
	private final GroupedMessageFilter filter;
	protected final FilterForGreater<Instant> leftBoundFilter;
	protected final FilterForLess<Instant> rightBoundFilter;
	protected PageInfo firstPage, lastPage;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	/** limit must be strictly positive ( limit greater than 0 ) */
	private final int limit;
	private final AtomicInteger returned;
	protected CassandraGroupedMessageFilter cassandraFilter;
	private final Order order;

	public GroupedMessageIteratorProvider(String requestInfo,
										  GroupedMessageFilter filter,
										  CassandraOperators operators,
										  BookInfo book,
										  ExecutorService composingService,
										  SelectQueryExecutor selectQueryExecutor,
										  Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
										  Order order) throws CradleStorageException {

		super(requestInfo);
		this.op = operators.getGroupedMessageBatchOperator();
		this.converter = operators.getGroupedMessageBatchEntityConverter();
		this.book = book;
		this.composingService = composingService;
		this.selectQueryExecutor = selectQueryExecutor;
		this.readAttrs = readAttrs;
		this.filter = filter;
		this.limit = filter.getLimit();
		this.returned = new AtomicInteger();
		this.leftBoundFilter = createLeftBoundFilter(filter);
		this.firstPage = FilterUtils.findFirstPage(filter.getPageId(), leftBoundFilter, book);
		this.rightBoundFilter = createRightBoundFilter(filter);
		this.lastPage = FilterUtils.findLastPage(filter.getPageId(), rightBoundFilter, book);
		this.order = order;

		// Filter should be initialized last as it might use above initialized properties
		this.cassandraFilter = createInitialFilter(filter);
	}

	private CassandraGroupedMessageFilter createInitialFilter(GroupedMessageFilter filter) {
		return new CassandraGroupedMessageFilter(
				getFirstPage().getId(),
				filter.getGroupName(),
				leftBoundFilter,
				rightBoundFilter,
				order,
				filter.getLimit());
	}

	//TODO refactor or split this method to avoid findFirstPage calculation multiple times.
	private FilterForGreater<Instant> createLeftBoundFilter(GroupedMessageFilter filter) throws CradleStorageException
	{
		FilterForGreater<Instant> result = filter.getFrom();
		var firstPageLocal = FilterUtils.findFirstPage(filter.getPageId(), result, book);
		Instant leftBoundFromPage = firstPageLocal.getStarted();
		if (result == null || (filter.getPageId() != null && leftBoundFromPage.isAfter(result.getValue())))
			return FilterForGreater.forGreaterOrEquals(leftBoundFromPage);

		// If the page wasn't specified in the filter, we should find a batch with a lower date,
		// which may contain messages that satisfy the original condition
		LocalDateTime leftBoundLocalDate = TimeUtils.toLocalTimestamp(result.getValue());
		LocalTime nearestBatchTime = getNearestBatchTime(
				firstPageLocal,
				filter.getGroupName(),
				leftBoundLocalDate.toLocalDate(),
				leftBoundLocalDate.toLocalTime());

		if (nearestBatchTime != null)
		{
			Instant nearestBatchInstant = TimeUtils.toInstant(leftBoundLocalDate.toLocalDate(), nearestBatchTime);
			if (nearestBatchInstant.isBefore(result.getValue()))
				result = FilterForGreater.forGreaterOrEquals(nearestBatchInstant);
		}

		return result;
	}

	private LocalTime getNearestBatchTime(PageInfo page, String groupAlias, LocalDate messageDate, LocalTime messageTime) throws CradleStorageException
	{
		while (page != null)
		{
			CompletableFuture<Row> future = op.getNearestTime(
					page.getId().getBookId().getName(),
					page.getName(),
					groupAlias,
					messageDate,
					messageTime,
					readAttrs);
			try
			{
				Row row = future.get();
				if (row != null)
					return row.getLocalTime(FIELD_FIRST_MESSAGE_TIME);
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Error while getting left bound ", e);
			}
			if (TimeUtils.toLocalTimestamp(page.getStarted()).toLocalDate().isBefore(messageDate))
				return null;
			page = book.getPreviousPage(page.getStarted());
		}

		return null;
	}

	//TODO refactor or split this method to avoid findLastPage calculation multiple times.
	protected FilterForLess<Instant> createRightBoundFilter(GroupedMessageFilter filter)
	{
		FilterForLess<Instant> result = filter.getTo();
		var lastPageLocal = FilterUtils.findLastPage(filter.getPageId(), result, book);
		Instant endOfPage = lastPageLocal.getEnded() == null ? Instant.now() : lastPageLocal.getEnded();

		return FilterForLess.forLessOrEquals(result == null || endOfPage.isBefore(result.getValue()) ? endOfPage : result.getValue());
	}

	protected CassandraGroupedMessageFilter createNextFilter(CassandraGroupedMessageFilter prevFilter, int updatedLimit) {
		PageInfo prevPage = book.getPage(prevFilter.getPageId());
		if (prevPage.equals(getLastPage()))
			return null;

		PageInfo nextPage = getNextPage(prevPage.getStarted());

		return new CassandraGroupedMessageFilter(
				nextPage.getId(),
				prevFilter.getGroupName(),
				prevFilter.getMessageTimeFrom(),
				prevFilter.getMessageTimeTo(),
				order,
				updatedLimit);
	}

	@Override
	public CompletableFuture<Iterator<StoredGroupedMessageBatch>> nextIterator() {
		if (cassandraFilter == null)
			return CompletableFuture.completedFuture(null);
		if (limit > 0 && returned.get() >= limit) {
			logger.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return CompletableFuture.completedFuture(null);
		}

		logger.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, selectQueryExecutor, getRequestInfo(), readAttrs)
				.thenApplyAsync(resultSet ->
				{
					PageId pageId = cassandraFilter.getPageId();
					// Updated limit should be smaller, since we already got entities from previous batch
					cassandraFilter = createNextFilter(cassandraFilter, Math.max(limit - returned.get(), 0));

					PagedIterator<GroupedMessageBatchEntity> pagedIterator = new PagedIterator<>(
							resultSet,
							selectQueryExecutor,
							converter::getEntity,
							getRequestInfo());

					return new ConvertingIterator<>(
							pagedIterator,
							entity -> MessagesWorker.mapGroupedMessageBatchEntity(pageId, entity));
				}, composingService)
				.thenApplyAsync(it -> new FilteredGroupedMessageBatchIterator(it, filter, limit, returned), composingService);
	}

	private boolean isDirectOrder() {
		return order == Order.DIRECT;
	}

	private PageInfo getFirstPage() {
		return isDirectOrder() ? firstPage : lastPage;
	}

	private PageInfo getLastPage() {
		return isDirectOrder() ? lastPage : firstPage;
	}

	private PageInfo getNextPage(Instant currentPageStart) {
		return isDirectOrder()
				? book.getNextPage(currentPageStart)
				: book.getPreviousPage(currentPageStart);
	}
}
