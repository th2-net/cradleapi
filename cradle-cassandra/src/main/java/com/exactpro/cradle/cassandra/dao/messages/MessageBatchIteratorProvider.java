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
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.utils.CassandraTimeUtils;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.*;
import com.exactpro.cradle.messages.MessageBatch;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGE_TIME;

public class MessageBatchIteratorProvider extends IteratorProvider<StoredMessageBatch>
{
	private static final Logger logger = LoggerFactory.getLogger(MessageBatchIteratorProvider.class);

	private final MessageBatchOperator op;
	private final BookInfo book;
	private final FilterForGreater<Instant> leftBoundFilter;
	private final FilterForLess<Instant> rightBoundFilter;
	private final String lastPart;
	private PageInfo firstPage;
	private String firstPart;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private final int limit;
	private final AtomicInteger returned;
	private final AtomicBoolean isLastPartReached = new AtomicBoolean(false);
	private CassandraStoredMessageFilter cassandraFilter;


	public MessageBatchIteratorProvider(String requestInfo, StoredMessageFilter filter, BookOperators ops, BookInfo book,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException
	{
		super(requestInfo);
		this.op = ops.getMessageBatchOperator();
		this.book = book;
		this.readAttrs = readAttrs;
		this.limit = filter.getLimit();
		this.returned = new AtomicInteger();
		this.leftBoundFilter = createLeftBoundFilter(filter);
		this.rightBoundFilter = createRightBoundFilter(filter);
		this.cassandraFilter = createInitialFilter(filter);
		this.lastPart = CassandraTimeUtils.getPart(TimeUtils.toLocalTimestamp(rightBoundFilter.getValue()));
	}

	private FilterForLess<Instant> createRightBoundFilter(StoredMessageFilter filter)
	{
		Instant rightBoundFromFilter = getRightBoundFromFilter(filter);
		FilterForGreater<Instant> result = FilterForGreater.forGreaterOrEquals(rightBoundFromFilter);
		PageInfo page = FilterUtils.findPage(getPageId(filter), result, book);
		Instant endOfPage = page.getEnded() == null ? Instant.now() : page.getEnded();

		return FilterForLess.forLessOrEquals(endOfPage.isBefore(rightBoundFromFilter) ? endOfPage : rightBoundFromFilter);
	}

	private FilterForGreater<Instant> createLeftBoundFilter(StoredMessageFilter filter) throws CradleStorageException
	{
		Instant leftBoundFromFilter = getLeftBoundFromFilter(filter);
		FilterForGreater<Instant> result = leftBoundFromFilter == null ? null : FilterForGreater.forGreaterOrEquals(leftBoundFromFilter);

		firstPage = FilterUtils.findPage(getPageId(filter), result, book);
		Instant leftBoundFromPage = firstPage.getStarted();
		if (result == null)
		{
			firstPart = CassandraTimeUtils.getPart(TimeUtils.toLocalTimestamp(leftBoundFromPage));
			return FilterForGreater.forGreaterOrEquals(leftBoundFromPage);
		}

		result.setValue(leftBoundFromFilter.isAfter(leftBoundFromPage) ? leftBoundFromFilter : leftBoundFromPage);

		LocalDateTime leftBoundLocalDate = TimeUtils.toLocalTimestamp(result.getValue());
		firstPart = CassandraTimeUtils.getPart(leftBoundLocalDate);
		LocalTime nearestBatchTime = getNearestBatchTime(firstPage.getId().getName(), filter.getSessionAlias().getValue(),
				filter.getDirection().getValue().getLabel(), firstPart, leftBoundLocalDate.toLocalDate(),
				leftBoundLocalDate.toLocalTime());

		if (nearestBatchTime != null)
		{
			Instant nearestBatchInstant = TimeUtils.toInstant(leftBoundLocalDate.toLocalDate(), nearestBatchTime);
			if (nearestBatchInstant.isBefore(result.getValue()))
			{
				result.setValue(nearestBatchInstant);
				firstPart = CassandraTimeUtils.getPart(leftBoundLocalDate);
			}
		}

		return result;
	}

	private CassandraStoredMessageFilter createInitialFilter(StoredMessageFilter filter)
	{
		return new CassandraStoredMessageFilter(firstPage.getId().getName(), filter.getSessionAlias().getValue(),
				filter.getDirection().getValue().getLabel(), firstPart, leftBoundFilter, rightBoundFilter, filter.getMessageId());
	}

	private Instant getLeftBoundFromFilter(StoredMessageFilter filter)
	{
		FilterForAny<StoredMessageId> messageId = filter.getMessageId();
		Instant result = null;
		if (messageId != null && (messageId.getOperation() == ComparisonOperation.GREATER_OR_EQUALS
				|| messageId.getOperation() == ComparisonOperation.GREATER))
		{
			result = messageId.getValue().getTimestamp();
		}
		if (filter.getTimestampFrom() != null)
		{
			Instant value = filter.getTimestampFrom().getValue();
			result = (result == null || result.isBefore(value)) ? value : result;
		}
		if (filter.getPageId() != null)
		{
			PageId pageId = filter.getPageId().getValue();
			PageInfo pageInfo = book.getPage(pageId);
			result = (result == null || result.isBefore(pageInfo.getStarted())) ? pageInfo.getStarted() : result;
		}

		return result;
	}

	private Instant getRightBoundFromFilter(StoredMessageFilter filter)
	{
		FilterForAny<StoredMessageId> messageId = filter.getMessageId();
		Instant result = null;
		if (messageId != null && (messageId.getOperation() == ComparisonOperation.LESS_OR_EQUALS
				|| messageId.getOperation() == ComparisonOperation.LESS))
		{
			result = messageId.getValue().getTimestamp();
		}
		if (filter.getTimestampTo() != null)
		{
			Instant tsTo = filter.getTimestampTo().getValue();
			result = result == null ? tsTo : (result.isBefore(tsTo) ? result : tsTo);
		}
		if (filter.getPageId() != null)
		{
			PageId pageId = filter.getPageId().getValue();
			PageInfo pageInfo = book.getPage(pageId);
			Instant ended = pageInfo.getEnded() == null ? Instant.now() : pageInfo.getEnded();
			result = result == null ? ended : (result.isBefore(ended) ? result : ended);
		}

		return result == null ? Instant.now() : result;
	}

	private LocalTime getNearestBatchTime(String page, String sessionAlias, String direction, String part,
			LocalDate messageDate, LocalTime messageTime) throws CradleStorageException
	{
		CompletableFuture<Row> future = op.getNearestTime(page, sessionAlias, direction, part, messageDate, messageTime, readAttrs);
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

	private PageId getPageId(StoredMessageFilter filter)
	{
		FilterForEquals<PageId> pageIdFilter = filter.getPageId();

		return pageIdFilter == null ? null : pageIdFilter.getValue();
	}

	@Override
	public CompletableFuture<Iterator<StoredMessageBatch>> nextIterator()
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
							return MessageEntityUtils.toStoredMessageBatch(entities, pageId);
						}
						catch (Exception e)
						{
							throw new RuntimeException("Error while converting message batch entity into stored message batch", e);
						}
					});
				});
	}

	private CassandraStoredMessageFilter createNextFilter(CassandraStoredMessageFilter prevFilter)
	{
		if (isLastPartReached.get())
			return null;

		String part = prevFilter.getPart();
		PageInfo page = book.getPage(new PageId(book.getId(), prevFilter.getPage()));

		//Was queried partition the last one for current page?
		if (page.getEnded() == null || !CassandraTimeUtils.getLastPart(page).equals(part))
		{
			part = CassandraTimeUtils.getNextPart(prevFilter.getPart());
		}
		else
		{
			page = book.getNextPage(page.getStarted());
			part = CassandraTimeUtils.getPart(TimeUtils.toLocalTimestamp(page.getStarted()));
		}

		isLastPartReached.compareAndSet(false, part.compareTo(lastPart) > -1);

		return new CassandraStoredMessageFilter(page.getId().getName(), prevFilter.getSessionAlias(),
				prevFilter.getDirection(), part, leftBoundFilter, rightBoundFilter, prevFilter.getMessageId());
	}
}
