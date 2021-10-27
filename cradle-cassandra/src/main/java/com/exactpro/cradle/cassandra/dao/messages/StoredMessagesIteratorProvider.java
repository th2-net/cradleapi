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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
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

import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGE_TIME;

public class StoredMessagesIteratorProvider extends AbstractMessageIteratorProvider<StoredMessage>
{
	private static final Logger logger = LoggerFactory.getLogger(StoredMessagesIteratorProvider.class);

	public StoredMessagesIteratorProvider(String requestInfo, StoredMessageFilter filter, BookOperators ops, BookInfo book,
			ExecutorService composingService,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException
	{
		super(requestInfo, filter, ops, book, composingService, readAttrs);
	}

	@Override
	protected FilterForGreater<Instant> createLeftBoundFilter(StoredMessageFilter filter) throws CradleStorageException
	{
		FilterForGreater<Instant> result = super.createLeftBoundFilter(filter);
		LocalDateTime leftBoundLocalDate = TimeUtils.toLocalTimestamp(result.getValue());
		//A batch with a smaller date can contain messages that satisfy the original condition
		LocalTime nearestBatchTime = getNearestBatchTime(firstPage.getId().getName(), filter.getSessionAlias().getValue(),
				filter.getDirection().getValue().getLabel(), leftBoundLocalDate.toLocalDate(),
				leftBoundLocalDate.toLocalTime());

		if (nearestBatchTime != null)
		{
			Instant nearestBatchInstant = TimeUtils.toInstant(leftBoundLocalDate.toLocalDate(), nearestBatchTime);
			if (nearestBatchInstant.isBefore(result.getValue()))
				result.setValue(nearestBatchInstant);
		}

		return result;
	}

	private LocalTime getNearestBatchTime(String page, String sessionAlias, String direction,
			LocalDate messageDate, LocalTime messageTime) throws CradleStorageException
	{
		CompletableFuture<Row> future = op.getNearestTime(page, sessionAlias, direction, messageDate, messageTime, readAttrs);
		try
		{
			Row row = future.get();
			return row == null ? null : row.getLocalTime(MESSAGE_TIME);
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error while getting left bound ", e);
		}
	}

	@Override
	public CompletableFuture<Iterator<StoredMessage>> nextIterator()
	{
		if (cassandraFilter == null)
			return CompletableFuture.completedFuture(null);
		if (limit > 0 && returned.get() >= limit)
		{
			logger.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return CompletableFuture.completedFuture(null);
		}

		logger.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, composingService, readAttrs)
				.thenApplyAsync(resultSet -> {
					PageId pageId = new PageId(book.getId(), cassandraFilter.getPage());
					cassandraFilter = createNextFilter(cassandraFilter);
					return new ConvertingPagedIterator<StoredMessageBatch, MessageBatchEntity>(resultSet, -1, new AtomicInteger(), entity -> {
						try
						{
							return entity.toStoredMessageBatch(pageId);
						}
						catch (Exception e)
						{
							throw new RuntimeException("Error while converting message batch entity into stored message batch", e);
						}
					});
				}, composingService)
				.thenApplyAsync(it -> new FilteredMessageIterator(it, filter, limit, returned), composingService);
	}
}
