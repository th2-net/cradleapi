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

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.messages.converters.MessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.dao.messages.sequences.MessageBatchIteratorCondition;
import com.exactpro.cradle.cassandra.dao.messages.sequences.MessageBatchIteratorFilter;
import com.exactpro.cradle.cassandra.dao.messages.sequences.SequenceRange;
import com.exactpro.cradle.cassandra.dao.messages.sequences.SequenceRangeExtractor;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.StorageUtils;
import com.exactpro.cradle.cassandra.workers.MessagesWorker;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.iterators.ConvertingIterator;
import com.exactpro.cradle.iterators.FilteringIterator;
import com.exactpro.cradle.iterators.TakeWhileIterator;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
import static java.util.Objects.requireNonNull;

abstract public class AbstractMessageIteratorProvider<T> extends IteratorProvider<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageIteratorProvider.class);
	protected final MessageBatchOperator op;
	protected final MessageBatchEntityConverter converter;
	protected final BookInfo book;
	protected final ExecutorService composingService;
	protected final SelectQueryExecutor selectQueryExecutor;
	protected final FilterForGreater<Instant> leftBoundFilter;
	protected final FilterForLess<Instant> rightBoundFilter;
	protected final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	protected final MessageFilter filter;
	/** limit must be strictly positive ( limit greater than 0 ) */
	protected final int limit;
	protected final AtomicInteger returned;
	protected final MessageBatchIteratorFilter<MessageBatchEntity> batchFilter;
	protected final MessageBatchIteratorCondition<MessageBatchEntity> iterationCondition;

	private final Iterator<PageInfo> pageProvider;
	// only get / set operation are guarded by lock
	private TakeWhileIterator<MessageBatchEntity> takeWhileIterator;

	public AbstractMessageIteratorProvider(String requestInfo, MessageFilter filter, CassandraOperators operators, BookInfo book,
										   ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
										   Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException
	{
		super(requestInfo);
		this.op = operators.getMessageBatchOperator();
		this.converter = operators.getMessageBatchEntityConverter();
		this.book = book;
		this.composingService = composingService;
		this.selectQueryExecutor = selectQueryExecutor;
		this.readAttrs = readAttrs;
		this.filter = filter;
		this.limit = filter.getLimit();
		this.returned = new AtomicInteger();
		this.leftBoundFilter = createLeftBoundFilter(filter);
		this.rightBoundFilter = createRightBoundFilter(filter);

		FilterForAny<Long> sequenceFilter = filter.getSequence();
		MessageBatchIteratorFilter<MessageBatchEntity> batchFilter;
		MessageBatchIteratorCondition<MessageBatchEntity> iterationCondition;
		if (sequenceFilter == null) {
			batchFilter = MessageBatchIteratorFilter.none();
			iterationCondition = MessageBatchIteratorCondition.none();
		} else {
			SequenceRangeExtractor<MessageBatchEntity> extractor = entity -> new SequenceRange(
					entity.getSequence(),
					entity.getLastSequence());
			batchFilter = new MessageBatchIteratorFilter<>(filter, extractor);
			iterationCondition = new MessageBatchIteratorCondition<>(filter, extractor);
		}

		this.batchFilter = batchFilter;
		this.iterationCondition = iterationCondition;

		this.pageProvider = book.getPages(
				requireNonNull(leftBoundFilter.getValue()),
				requireNonNull(rightBoundFilter.getValue()),
				filter.getOrder()
		);
	}

	protected FilterForGreater<Instant> createLeftBoundFilter(MessageFilter filter) throws CradleStorageException {
		FilterForGreater<Instant> result = filter.getTimestampFrom();
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
		LocalTime nearestBatchTime = getNearestBatchTime(leftBoundFromPage, filter.getSessionAlias(),
				filter.getDirection().getLabel(), leftBoundLocalDate.toLocalDate(),
				leftBoundLocalDate.toLocalTime());

		if (nearestBatchTime != null) {
			Instant nearestBatchInstant = TimeUtils.toInstant(leftBoundLocalDate.toLocalDate(), nearestBatchTime);
			if (nearestBatchInstant.isBefore(result.getValue())) {
				result = FilterForGreater.forGreaterOrEquals(nearestBatchInstant);
			}
		}
		return result;
	}

	private LocalTime getNearestBatchTime(Instant timestamp,
										  String sessionAlias,
										  String direction,
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
					sessionAlias,
					direction,
					messageDate,
					messageTime,
					readAttrs);
			try {
				Row row = future.get();
				if (row != null) {
					return row.getLocalTime(FIELD_FIRST_MESSAGE_TIME);
				}
			} catch (Exception e) {
				throw new CradleStorageException("Error while getting left bound ", e);
			}
			if (StorageUtils.toLocalDateTime(page.getStarted()).toLocalDate().isBefore(messageDate)) {
				return null;
			}
		}

		return null;
	}

	protected FilterForLess<Instant> createRightBoundFilter(MessageFilter filter) {
		FilterForLess<Instant> result = filter.getTimestampTo();
		Instant lastTimestamp = findLastTimestamp(filter.getPageId(), result, book);
		Instant endTimestamp = lastTimestamp == null ? Instant.now() : lastTimestamp;

		return FilterForLess.forLessOrEquals(result == null || endTimestamp.isBefore(result.getValue())
				? endTimestamp
				: result.getValue());
	}

	protected CassandraStoredMessageFilter createFilter(@Nonnull PageInfo pageInfo, int updatedLimit) {
		return new CassandraStoredMessageFilter(
				pageInfo.getId(),
				filter.getSessionAlias(),
				filter.getDirection().getLabel(),
				leftBoundFilter,
				rightBoundFilter,
				updatedLimit,
				filter.getOrder());
	}

	protected @Nullable PageInfo nextPage() {
		if (takeWhileIterator != null && takeWhileIterator.isHalted()) {
			LOGGER.debug("Iterator was interrupted because iterator condition was not met");
			return null;
		}

		if (limit > 0 && returned.get() >= limit) {
			LOGGER.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return null;
		}

		return pageProvider.hasNext() ? pageProvider.next() : null;
	}

	protected Iterator<StoredMessageBatch> getBatchedIterator(PageId pageId,
															  MappedAsyncPagingIterable<MessageBatchEntity> resultSet) {
		PagedIterator<MessageBatchEntity> pagedIterator = new PagedIterator<>(
				resultSet,
				selectQueryExecutor,
				converter::getEntity,
				getRequestInfo());
		FilteringIterator<MessageBatchEntity> filteringIterator = new FilteringIterator<>(
				pagedIterator,
				batchFilter::test);

		// We need to store this iterator since
		// it gives info whether or no iterator was halted
		takeWhileIterator = new TakeWhileIterator<>(
				filteringIterator,
				iterationCondition);

		return new ConvertingIterator<>(
				takeWhileIterator, entity ->
				MessagesWorker.mapMessageBatchEntity(pageId, entity));
	}
}
