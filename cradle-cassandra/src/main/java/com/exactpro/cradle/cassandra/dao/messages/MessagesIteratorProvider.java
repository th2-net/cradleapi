/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.cassandra.dao.messages.sequences.MessageBatchIteratorCondition;
import com.exactpro.cradle.cassandra.dao.messages.sequences.MessageBatchIteratorFilter;
import com.exactpro.cradle.cassandra.dao.messages.sequences.SequenceRange;
import com.exactpro.cradle.cassandra.dao.messages.sequences.SequenceRangeExtractor;
import com.exactpro.cradle.cassandra.iterators.PagedIterator;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.iterators.ConvertingIterator;
import com.exactpro.cradle.iterators.FilteringIterator;
import com.exactpro.cradle.iterators.LimitedIterator;
import com.exactpro.cradle.iterators.TakeWhileIterator;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.workers.MessagesWorker.mapMessageBatchEntity;

public class MessagesIteratorProvider extends AbstractMessageIteratorProvider<StoredMessage> {
	private static final Logger logger = LoggerFactory.getLogger(MessagesIteratorProvider.class);

	private final MessageBatchIteratorFilter<StoredMessageBatch> batchFilter;
	private final MessageBatchIteratorCondition<StoredMessageBatch> iterationCondition;
	private TakeWhileIterator<StoredMessageBatch> iterator;

	public MessagesIteratorProvider(String requestInfo, MessageFilter filter, CassandraOperators operators, BookInfo book,
									ExecutorService composingService, SelectQueryExecutor selectQueryExecutor,
									Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) throws CradleStorageException
	{
		super(requestInfo, filter, operators, book, composingService, selectQueryExecutor, readAttrs);
		FilterForAny<Long> sequenceFilter = filter.getSequence();
		if (sequenceFilter == null) {
			batchFilter = MessageBatchIteratorFilter.none();
			iterationCondition = MessageBatchIteratorCondition.none();
		} else {
			SequenceRangeExtractor<StoredMessageBatch> extractor = batch -> new SequenceRange(batch.getFirstMessage().getSequence(),
					batch.getLastMessage().getSequence());
			batchFilter = new MessageBatchIteratorFilter<>(filter, extractor);
			iterationCondition = new MessageBatchIteratorCondition<>(filter, extractor);
		}
	}

	@Override
	public CompletableFuture<Iterator<StoredMessage>> nextIterator()
	{
		if (cassandraFilter == null) {
			return CompletableFuture.completedFuture(null);
		}

		if (iterator != null && iterator.isHalted()) {
			logger.debug("Iterator was interrupted because iterator condition was not met");
			return CompletableFuture.completedFuture(null);
		}

		if (limit > 0 && returned.get() >= limit) {
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

					PagedIterator<MessageBatchEntity> pagedIterator = new PagedIterator<>(
							resultSet,
							selectQueryExecutor,
							messageBatchEntityConverter::getEntity,
							getRequestInfo());
					ConvertingIterator<MessageBatchEntity, StoredMessageBatch> convertingIterator = new ConvertingIterator<>(
							pagedIterator, entity ->
							mapMessageBatchEntity(pageId, entity));
					FilteringIterator<StoredMessageBatch> filteringIterator = new FilteringIterator<>(
							convertingIterator,
							batchFilter::test);
					iterator = new TakeWhileIterator<>(
							filteringIterator,
							iterationCondition);

					return iterator;
				}, composingService)
				.thenApplyAsync(it -> new FilteredMessageIterator(it, filter, limit, returned), composingService);
	}
}
