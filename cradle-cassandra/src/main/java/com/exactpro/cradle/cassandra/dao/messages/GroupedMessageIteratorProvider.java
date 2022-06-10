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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.dao.messages.converters.GroupedMessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.cassandra.workers.MessagesWorker;
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class GroupedMessageIteratorProvider extends IteratorProvider<StoredGroupedMessageBatch>
{
	public static final Logger logger = LoggerFactory.getLogger(GroupedMessageIteratorProvider.class);
	
	private final GroupedMessageBatchOperator op;
	private final GroupedMessageBatchEntityConverter converter;
	private final BookInfo book;
	private final ExecutorService composingService;
	private final SelectQueryExecutor selectQueryExecutor;
	private final GroupedMessageFilter filter;
	private PageInfo firstPage, lastPage;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	private final int limit;
	private final AtomicInteger returned;
	protected CassandraGroupedMessageFilter cassandraFilter;
	
	public GroupedMessageIteratorProvider(String requestInfo,
			GroupedMessageFilter filter, BookOperators ops, BookInfo book,
			ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException
	{

		super(requestInfo);
		this.op = ops.getGroupedMessageBatchOperator();
		this.converter = ops.getGroupedMessageBatchEntityConverter();
		this.book = book;
		this.composingService = composingService;
		this.selectQueryExecutor = selectQueryExecutor;
		this.readAttrs = readAttrs;
		this.filter = filter;
		this.limit = filter.getLimit();
		this.returned = new AtomicInteger();
		// TODO: Get message batch before *from* timestamp
		this.firstPage = FilterUtils.findFirstPage(filter.getPageId(), filter.getFrom(), book);
		this.lastPage = FilterUtils.findLastPage(filter.getPageId(), filter.getTo(), book);
		this.cassandraFilter = createInitialFilter(filter);
	}

	private CassandraGroupedMessageFilter createInitialFilter(GroupedMessageFilter filter)
	{
		return new CassandraGroupedMessageFilter(firstPage.getId().getName(), filter.getGroupName(),
				filter.getFrom(), filter.getTo(), filter.getLimit());
	}

	protected CassandraGroupedMessageFilter createNextFilter(CassandraGroupedMessageFilter prevFilter, int updatedLimit)
	{
		PageInfo prevPage = book.getPage(new PageId(book.getId(), prevFilter.getPage()));
		if (prevPage.equals(lastPage))
			return null;

		PageInfo nextPage = book.getNextPage(prevPage.getStarted());

		return new CassandraGroupedMessageFilter(nextPage.getId().getName(), prevFilter.getGroupName(),
				prevFilter.getMessageTimeFrom(), prevFilter.getMessageTimeTo(), updatedLimit);
	}

	@Override
	public CompletableFuture<Iterator<StoredGroupedMessageBatch>> nextIterator()
	{
		if (cassandraFilter == null)
			return CompletableFuture.completedFuture(null);
		if (limit > 0 && returned.get() >= limit)
		{
			logger.debug("Filtering interrupted because limit for records to return ({}) is reached ({})", limit, returned);
			return CompletableFuture.completedFuture(null);
		}

		logger.debug("Getting next iterator for '{}' by filter {}", getRequestInfo(), cassandraFilter);
		return op.getByFilter(cassandraFilter, selectQueryExecutor, getRequestInfo(), readAttrs)
				.thenApplyAsync(resultSet ->
				{
					PageId pageId = new PageId(book.getId(), cassandraFilter.getPage());
					// Updated limit should be smaller, since we already got entities from previous batch
					cassandraFilter = createNextFilter(cassandraFilter, Math.max(limit - returned.get(),0));
					return new ConvertingPagedIterator<>(resultSet, selectQueryExecutor, limit, returned,
							entity -> MessagesWorker.mapGroupedMessageBatchEntity(pageId, entity), converter::getEntity,
							"fetch next page of message batches");
				}, composingService)
				.thenApplyAsync(it -> new FilteredGroupedMessageBatchIterator(it, filter, limit, returned), composingService);
	}
}
