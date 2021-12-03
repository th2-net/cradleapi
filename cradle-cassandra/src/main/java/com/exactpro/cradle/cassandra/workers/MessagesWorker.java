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

package com.exactpro.cradle.cassandra.workers;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.BookOperators;
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageSession;
import com.exactpro.cradle.cassandra.dao.cache.CachedSession;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.exactpro.cradle.CradleStorage.EMPTY_MESSAGE_INDEX;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class MessagesWorker extends Worker
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesWorker.class);

	public MessagesWorker(CassandraStorageSettings settings, CradleOperators ops,
			ExecutorService composingService, BookAndPageChecker bpc,
			Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		super(settings, ops, composingService, bpc, writeAttrs, readAttrs);
	}

	public CompletableFuture<CradleResultSet<StoredMessageBatch>> getMessageBatches(MessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		MessageBatchesIteratorProvider provider =
				new MessageBatchesIteratorProvider("get messages batches filtered by " + filter, filter,
						getBookOps(book.getId()), book, composingService, readAttrs);
		return provider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
	}

	public CompletableFuture<CradleResultSet<StoredMessage>> getMessages(MessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		MessagesIteratorProvider provider =
				new MessagesIteratorProvider("get messages filtered by " + filter, filter,
						ops.getOperators(book.getId()), book, composingService, readAttrs);
		return provider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
	}

	private CompletableFuture<Row> getNearestTimeAndSequenceBefore(BookInfo bookInfo, PageInfo page,
			MessageBatchOperator mbOperator, String sessionAlias, String direction, LocalDate messageDate,
			LocalTime messageTime, long sequence, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		// Message batch can't contain messages from different dates. This is a business requirement.
		// Therefore, when current page has different start date, search in previous pages doesn't make sense
		if (page == null || TimeUtils.toLocalTimestamp(page.getStarted()).toLocalDate().isBefore(messageDate))
			return CompletableFuture.completedFuture(null);
		return mbOperator.getNearestTimeAndSequenceBefore(page.getId().getName(), sessionAlias, direction,
						messageDate, messageTime, sequence, readAttrs)
				.thenComposeAsync(row ->
				{
					if (row != null)
						return CompletableFuture.completedFuture(row);
					// We continue searching in previous pages
					PageInfo previousPage = bookInfo.getPreviousPage(page.getStarted());
					return getNearestTimeAndSequenceBefore(bookInfo, previousPage, mbOperator, sessionAlias, direction,
							messageDate, messageTime, sequence, readAttrs);
				}, composingService);
	}

	public CompletableFuture<StoredMessageBatch> getMessageBatch(StoredMessageId id, PageId pageId)
	{
		logger.debug("Getting message batch for message with id '{}'", id);
		BookId bookId = pageId.getBookId();
		BookInfo bookInfo;
		try
		{
			bookInfo = getBook(bookId);
		}
		catch (CradleStorageException e)
		{
			return CompletableFutures.failedFuture(e);
		}

		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
		BookOperators bookOps = getBookOps(bookId);
		MessageBatchOperator mbOperator = bookOps.getMessageBatchOperator();

		return getNearestTimeAndSequenceBefore(bookInfo, bookInfo.getPage(pageId), mbOperator, id.getSessionAlias(),
				id.getDirection().getLabel(), ldt.toLocalDate(), ldt.toLocalTime(), id.getSequence(), readAttrs)
				.thenComposeAsync(row ->
				{
					if (row == null)
					{
						logger.debug("No message batches found by id '{}'", id);
						return CompletableFuture.completedFuture(null);
					}
					return mbOperator
							.get(pageId.getName(), id.getSessionAlias(), id.getDirection().getLabel(),
									ldt.toLocalDate(), row.getLocalTime(MESSAGE_TIME), row.getLong(SEQUENCE), readAttrs)
							.thenApplyAsync(e ->
							{
								if (e == null)
									return null;

								try
								{
									StoredMessageBatch batch = e.toStoredMessageBatch(pageId);
									logger.debug("Message batch with id '{}' found for message with id '{}'",
											batch.getId(), id);
									return batch;
								}
								catch (Exception ex)
								{
									throw new CompletionException(ex);
								}
							}, composingService);
				}, composingService);
	}

	public CompletableFuture<StoredMessage> getMessage(StoredMessageId id, PageId pageId)
	{
		return getMessageBatch(id, pageId)
				.thenComposeAsync(batch ->
				{
					if (batch == null)
						return CompletableFuture.completedFuture(null);

					Optional<StoredMessage>
							found = batch.getMessages().stream().filter(m -> id.equals(m.getId())).findFirst();
					if (found.isPresent())
						return CompletableFuture.completedFuture(found.get());

					logger.debug("There is no message with id '{}' in batch '{}'", id, batch.getId());
					return CompletableFuture.completedFuture(null);
				}, composingService);
	}

	public MessageBatchEntity createEntity(MessageBatchToStore batch, PageId pageId) throws IOException
	{
		return new MessageBatchEntity(batch, pageId, settings.getMaxUncompressedMessageBatchSize());
	}

	public CompletableFuture<PageSessionEntity> storePageSession(MessageBatchToStore batch, PageId pageId)
	{
		StoredMessageId batchId = batch.getId();
		BookOperators bookOps = getBookOps(pageId.getBookId());
		CachedPageSession cachedPageSession = new CachedPageSession(pageId.toString(),
				batchId.getSessionAlias(), batchId.getDirection().getLabel());
		if (!bookOps.getPageSessionsCache().store(cachedPageSession))
		{
			logger.debug("Skipped writing page/session of message batch '{}'", batchId);
			return CompletableFuture.completedFuture(null);
		}

		logger.debug("Writing page/session of batch '{}'", batchId);

		return bookOps.getPageSessionsOperator().write(new PageSessionEntity(batchId, pageId), writeAttrs);
	}

	public CompletableFuture<SessionEntity> storeSession(MessageBatchToStore batch)
	{
		StoredMessageId batchId = batch.getId();
		BookId bookId = batchId.getBookId();
		BookOperators bookOps = getBookOps(bookId);
		CachedSession cachedSession = new CachedSession(bookId.toString(), batch.getSessionAlias());
		if (!bookOps.getSessionsCache().store(cachedSession))
		{
			logger.debug("Skipped writing book/session of message batch '{}'", batchId);
			return CompletableFuture.completedFuture(null);
		}
		logger.debug("Writing book/session of batch '{}'", batchId);

		return bookOps.getSessionsOperator().write(new SessionEntity(bookId.toString(), batch.getSessionAlias()), writeAttrs);
	}

	public CompletableFuture<MessageBatchEntity> storeMessageBatch(MessageBatchEntity entity, BookId bookId)
	{
		BookOperators bookOps = getBookOps(bookId);
		MessageBatchOperator mbOperator = bookOps.getMessageBatchOperator();

		return mbOperator.write(entity, writeAttrs);
	}

	public long getBoundarySequence(String sessionAlias, Direction direction, BookInfo book, boolean first)
			throws CradleStorageException
	{
		MessageBatchOperator mbOp = getBookOps(book.getId()).getMessageBatchOperator();
		PageInfo currentPage = first ? book.getFirstPage() : book.getLastPage();
		Row row = null;

		while (row == null && currentPage != null)
		{
			try
			{
				row = first ? mbOp.getFirstSequence(currentPage.getId().getName(), sessionAlias, direction.getLabel(),
						readAttrs).get()
						: mbOp.getLastSequence(currentPage.getId().getName(), sessionAlias, direction.getLabel(),
								readAttrs).get();
			}
			catch (InterruptedException | ExecutionException e)
			{
				String msg = String.format("Error occurs while getting %s sequence for page '%s', session alias '%s', " +
						"direction '%s'", first ? "first" : "last", currentPage.getId().getName(), sessionAlias, direction);
				throw new CradleStorageException(msg, e);
			}

			if (row == null)
				currentPage = first ? book.getNextPage(currentPage.getStarted())
						: book.getPreviousPage(currentPage.getStarted());
		}
		if (row == null)
		{
			logger.debug("There is no messages yet in book '{}' with session alias '{}' and direction '{}'", book.getId(),
					sessionAlias, direction);
			return EMPTY_MESSAGE_INDEX;
		}

		return row.getLong(first ? SEQUENCE : LAST_SEQUENCE);
	}
}