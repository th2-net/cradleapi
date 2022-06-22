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
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.workers.MessagesWorker.mapMessageBatchEntity;

public class MessagesIteratorProvider extends AbstractMessageIteratorProvider<StoredMessage>
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesIteratorProvider.class);

	public MessagesIteratorProvider(String requestInfo, MessageFilter filter, CassandraOperators operators, BookInfo book,
									ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
									Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException
	{
		super(requestInfo, filter, operators, book, composingService, selectQueryExecutor, readAttrs);
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
		return op.getByFilter(cassandraFilter, selectQueryExecutor, getRequestInfo(), readAttrs)
				.thenApplyAsync(resultSet ->
				{
					PageId pageId = new PageId(book.getId(), cassandraFilter.getPage());
					// Updated limit should be smaller, since we already got entities from previous batch
					cassandraFilter = createNextFilter(cassandraFilter, limit - returned.get());
					return new ConvertingPagedIterator<>(resultSet, selectQueryExecutor, -1, new AtomicInteger(),
							entity -> mapMessageBatchEntity(pageId, entity), messageBatchEntityConverter::getEntity,
							"fetch next page of message batches");
				}, composingService)
				.thenApplyAsync(it -> new FilteredMessageIterator(it, filter, limit, returned), composingService);
	}
}
