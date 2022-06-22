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
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.messages.converters.MessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.FIELD_FIRST_MESSAGE_TIME;

abstract public class AbstractMessageIteratorProvider<T> extends IteratorProvider<T>
{
	protected final MessageBatchOperator op;
	protected final MessageBatchEntityConverter messageBatchEntityConverter;
	protected final BookInfo book;
	protected final ExecutorService composingService;
	protected final SelectQueryExecutor selectQueryExecutor;
	protected final FilterForGreater<Instant> leftBoundFilter;
	protected final FilterForLess<Instant> rightBoundFilter;
	protected PageInfo firstPage, lastPage;
	protected final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	protected final MessageFilter filter;
	protected final int limit;
	protected final AtomicInteger returned;
	protected CassandraStoredMessageFilter cassandraFilter;

	public AbstractMessageIteratorProvider(String requestInfo, MessageFilter filter, CassandraOperators operators, BookInfo book,
										   ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
										   Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException
	{
		super(requestInfo);
		this.op = operators.getMessageBatchOperator();
		this.messageBatchEntityConverter = operators.getMessageBatchEntityConverter();
		this.book = book;
		this.composingService = composingService;
		this.selectQueryExecutor = selectQueryExecutor;
		this.readAttrs = readAttrs;
		this.filter = filter;
		this.limit = filter.getLimit();
		this.returned = new AtomicInteger();
		this.leftBoundFilter = createLeftBoundFilter(filter);
		this.rightBoundFilter = createRightBoundFilter(filter);
		this.cassandraFilter = createInitialFilter(filter);
	}

	protected FilterForGreater<Instant> createLeftBoundFilter(MessageFilter filter) throws CradleStorageException
	{
		FilterForGreater<Instant> result = filter.getTimestampFrom();
		firstPage = FilterUtils.findFirstPage(filter.getPageId(), result, book);
		Instant leftBoundFromPage = firstPage.getStarted();
		if (result == null || (filter.getPageId() != null && leftBoundFromPage.isAfter(result.getValue())))
			return FilterForGreater.forGreaterOrEquals(leftBoundFromPage);

		// If the page wasn't specified in the filter, we should find a batch with a lower date,
		// which may contain messages that satisfy the original condition
		LocalDateTime leftBoundLocalDate = TimeUtils.toLocalTimestamp(result.getValue());
		LocalTime nearestBatchTime = getNearestBatchTime(firstPage, filter.getSessionAlias(),
				filter.getDirection().getLabel(), leftBoundLocalDate.toLocalDate(),
				leftBoundLocalDate.toLocalTime());

		if (nearestBatchTime != null)
		{
			Instant nearestBatchInstant = TimeUtils.toInstant(leftBoundLocalDate.toLocalDate(), nearestBatchTime);
			if (nearestBatchInstant.isBefore(result.getValue()))
				result = FilterForGreater.forGreaterOrEquals(nearestBatchInstant);
			firstPage = FilterUtils.findFirstPage(filter.getPageId(), result, book);
		}
		
		return result;
	}

	private LocalTime getNearestBatchTime(PageInfo page, String sessionAlias, String direction,
			LocalDate messageDate, LocalTime messageTime) throws CradleStorageException
	{
		while (page != null)
		{
			CompletableFuture<Row> future = op.getNearestTime(
					page.getId().getBookId().getName(),
					page.getId().getName(),
					sessionAlias,
					direction,
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

	protected FilterForLess<Instant> createRightBoundFilter(MessageFilter filter)
	{
		FilterForLess<Instant> result = filter.getTimestampTo();
		lastPage = FilterUtils.findLastPage(filter.getPageId(), result, book);
		Instant endOfPage = lastPage.getEnded() == null ? Instant.now() : lastPage.getEnded();

		return FilterForLess.forLessOrEquals(result == null || endOfPage.isBefore(result.getValue()) ? endOfPage : result.getValue());
	}

	protected CassandraStoredMessageFilter createInitialFilter(MessageFilter filter)
	{
		return new CassandraStoredMessageFilter(firstPage.getId().getBookId().getName(),firstPage.getId().getName(), filter.getSessionAlias(),
				filter.getDirection().getLabel(), leftBoundFilter, rightBoundFilter, filter.getSequence(), filter.getLimit());
	}

	protected CassandraStoredMessageFilter createNextFilter(CassandraStoredMessageFilter prevFilter, int updatedLimit)
	{
		PageInfo prevPage = book.getPage(new PageId(book.getId(), prevFilter.getPage()));
		if (prevPage.equals(lastPage))
			return null;

		PageInfo nextPage = book.getNextPage(prevPage.getStarted());

		return new CassandraStoredMessageFilter(nextPage.getId().getBookId().getName(), nextPage.getId().getName(), prevFilter.getSessionAlias(),
				prevFilter.getDirection(), leftBoundFilter, rightBoundFilter, prevFilter.getSequence(), updatedLimit);
	}
}
