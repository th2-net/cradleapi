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
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.messages.converters.GroupedMessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.StorageUtils;
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

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.exactpro.cradle.Order.REVERSE;
import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.FIELD_FIRST_MESSAGE_TIME;
import static com.exactpro.cradle.cassandra.utils.FilterUtils.findFirstTimestamp;
import static com.exactpro.cradle.cassandra.utils.FilterUtils.findLastTimestamp;
import static com.exactpro.cradle.filters.FilterForLess.forLessOrEquals;
import static java.lang.Math.max;

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
	private final Iterator<PageInfo> pageProvider;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	/** limit must be strictly positive ( limit greater than 0 ) */
	private final int limit;
	private final AtomicInteger returned;
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
		this.rightBoundFilter = createRightBoundFilter(filter);
		this.pageProvider = book.getPages(
			leftBoundFilter.getValue(),
			rightBoundFilter.getValue(),
			order
		);
		this.order = order;
	}

	private FilterForGreater<Instant> createLeftBoundFilter(GroupedMessageFilter filter) throws CradleStorageException
	{
		FilterForGreater<Instant> result = filter.getFrom();
		Instant leftBoundFromPage = findFirstTimestamp(filter.getPageId(), result, book);
		if (leftBoundFromPage == null) {
			if (result != null) {
				return result;
			}
			return FilterForGreater.forGreaterOrEquals(Instant.MIN);
		}
		if (result == null || (filter.getPageId() != null && leftBoundFromPage.isAfter(result.getValue()))) {
			return FilterForGreater.forGreaterOrEquals(leftBoundFromPage);
		}

		// If the page wasn't specified in the filter, we should find a batch with a lower date,
		// which may contain messages that satisfy the original condition
		LocalDateTime leftBoundLocalDate = StorageUtils.toLocalDateTime(result.getValue());
		LocalTime nearestBatchTime = getNearestBatchTime(
				leftBoundFromPage,
				filter.getGroupName(),
				leftBoundLocalDate.toLocalDate(),
				leftBoundLocalDate.toLocalTime());

		if (nearestBatchTime != null)
		{
			Instant nearestBatchInstant = TimeUtils.toInstant(leftBoundLocalDate.toLocalDate(), nearestBatchTime);
			if (nearestBatchInstant.isBefore(result.getValue())) {
				result = FilterForGreater.forGreaterOrEquals(nearestBatchInstant);
			}
		}

		return result;
	}

	private LocalTime getNearestBatchTime(Instant timestamp,
										  String groupAlias,
										  LocalDate messageDate,
										  LocalTime messageTime) throws CradleStorageException {
		if (timestamp == null) {
			return null;
		}
		Iterator<PageInfo> pageInfoIterator = book.getPages(null, timestamp, REVERSE);
		while (pageInfoIterator.hasNext()) {
			PageInfo page = pageInfoIterator.next();
			CompletableFuture<Row> future = op.getNearestTime(
					page.getBookName(),
					page.getName(),
					groupAlias,
					messageDate,
					messageTime,
					readAttrs);
			try {
				Row row = future.get();
				if (row != null)
					return row.getLocalTime(FIELD_FIRST_MESSAGE_TIME);
			} catch (Exception e) {
				throw new CradleStorageException("Error while getting left bound ", e);
			}
			if (StorageUtils.toLocalDateTime(page.getStarted()).toLocalDate().isBefore(messageDate)) {
				return null;
			}
		}

		return null;
	}

	@Override
	public CompletableFuture<Iterator<StoredGroupedMessageBatch>> nextIterator() {
		if (!pageProvider.hasNext()) {
			return CompletableFuture.completedFuture(null);
		}
		PageInfo nextPage = pageProvider.next();

		if (limit > 0 && returned.get() >= limit) {
			logger.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return CompletableFuture.completedFuture(null);
		}

		// There is no way to make queries with limit work 100% of the cases through cassandra query filtering with only first_message_time and first_message_date being clustering columns.
		// It is required to have last_message_time, last_message_date to make DB filtering possible
		// We have to rely on programmatic filtering for now

		// Example:
		// batch1: t1 - t1
		// batch2: t1 - t2
		//
		// start time filter >= t1 + 5
		// limit: 1
		//
		// If we put first_message_time >= t1 + 5 clause in cassandra query we will not receive both batches
		// If we put first_message_time >= t1 and LIMIT 1 clauses in cassandra query we will receive batch1 and we will programmatically filter it.
		// Only if we put first_message_time >= t1 without LIMIT clause we will receive batch1 and batch2 and we will be able to filter batch1 programmatically and return batch2 to user.

		CassandraGroupedMessageFilter cassandraFilter = createFilter(nextPage, 0);


		logger.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, selectQueryExecutor, getRequestInfo(), readAttrs)
				.thenApplyAsync(resultSet -> {
					PagedIterator<GroupedMessageBatchEntity> pagedIterator = new PagedIterator<>(
							resultSet,
							selectQueryExecutor,
							converter::getEntity,
							getRequestInfo());

					return new ConvertingIterator<>(
							pagedIterator,
							entity -> MessagesWorker.mapGroupedMessageBatchEntity(nextPage.getId(), entity));
				}, composingService)
				.thenApplyAsync(it -> new FilteredGroupedMessageBatchIterator(it, filter, limit, returned), composingService);
	}

	private FilterForLess<Instant> createRightBoundFilter(GroupedMessageFilter filter) {
		FilterForLess<Instant> result = filter.getTo();
		Instant lastTimestamp = findLastTimestamp(filter.getPageId(), result, book);
		Instant endTimestamp = lastTimestamp == null ? Instant.now() : lastTimestamp;

		return forLessOrEquals(result == null || endTimestamp.isBefore(result.getValue())
				? endTimestamp
				: result.getValue()
		);
	}

	private CassandraGroupedMessageFilter createFilter(@Nonnull PageInfo pageInfo, int updatedLimit) {
		return new CassandraGroupedMessageFilter(
				pageInfo.getId(),
				filter.getGroupName(),
				leftBoundFilter,
				rightBoundFilter,
				order,
				updatedLimit);
	}
}
