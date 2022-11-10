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

package com.exactpro.cradle.cassandra.workers;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.counters.MessageStatisticsCollector;
import com.exactpro.cradle.cassandra.counters.SessionStatisticsCollector;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageSession;
import com.exactpro.cradle.cassandra.dao.cache.CachedSession;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.messages.converters.MessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.utils.GroupedMessageEntityUtils;
import com.exactpro.cradle.cassandra.utils.MessageBatchEntityUtils;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import io.prometheus.client.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.zip.DataFormatException;

import static com.exactpro.cradle.CradleStorage.EMPTY_MESSAGE_INDEX;
import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.*;
import static java.lang.String.format;

public class MessagesWorker extends Worker
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesWorker.class);

	private static final Counter MESSAGE_READ_METRIC = Counter.build().name("cradle_message_readed")
			.help("Fetched messages").labelNames(BOOK_ID, SESSION_ALIAS, DIRECTION).register();
	private static final Counter MESSAGE_WRITE_METRIC = Counter.build().name("cradle_message_stored")
			.help("Stored messages").labelNames(BOOK_ID, SESSION_ALIAS, DIRECTION).register();

	private final MessageStatisticsCollector messageStatisticsCollector;
	private final SessionStatisticsCollector sessionStatisticsCollector;
	public MessagesWorker(WorkerSupplies workerSupplies
			, MessageStatisticsCollector messageStatisticsCollector
			, SessionStatisticsCollector sessionStatisticsCollector)
	{
		super(workerSupplies);
		this.messageStatisticsCollector = messageStatisticsCollector;
		this.sessionStatisticsCollector = sessionStatisticsCollector;
	}

	public static StoredMessageBatch mapMessageBatchEntity(PageId pageId, MessageBatchEntity entity)
	{
		try
		{
			StoredMessageBatch batch = MessageBatchEntityUtils.toStoredMessageBatch(entity, pageId);
			updateMessageReadMetrics(batch);
			return batch;
		}
		catch (DataFormatException | IOException e)
		{
			throw new CompletionException("Error while converting message batch entity into stored message batch", e);
		}
	}

	public static StoredGroupedMessageBatch mapGroupedMessageBatchEntity(PageId pageId, GroupedMessageBatchEntity entity)
	{
		try
		{
			StoredGroupedMessageBatch batch = GroupedMessageEntityUtils.toStoredGroupedMessageBatch(entity, pageId);
			updateMessageReadMetrics(pageId.getBookId(), batch);
			return batch;
		}
		catch (DataFormatException | IOException e)
		{
			throw new CompletionException("Error while converting message batch entity into stored message batch", e);
		}
	}

	private static void updateMessageReadMetrics(StoredMessageBatch batch)
	{
		MESSAGE_READ_METRIC
				.labels(batch.getId().getBookId().getName(), batch.getSessionAlias(), batch.getDirection().getLabel())
				.inc(batch.getMessageCount());
	}

	private static void updateMessageReadMetrics(BookId bookId, StoredGroupedMessageBatch batch)
	{
		MESSAGE_READ_METRIC
				.labels(bookId.getName(), batch.getGroup(), "")
				.inc(batch.getMessageCount());
	}

	private static void updateMessageWriteMetrics(MessageBatchEntity entity, BookId bookId)
	{
		MESSAGE_WRITE_METRIC
				.labels(bookId.getName(), entity.getSessionAlias(), entity.getDirection())
				.inc(entity.getMessageCount());
	}

	private static void updateMessageWriteMetrics(GroupedMessageBatchEntity entity, BookId bookId)
	{
		MESSAGE_WRITE_METRIC
				.labels(bookId.getName(), entity.getGroup(), "")
				.inc(entity.getMessageCount());
	}

	public CompletableFuture<CradleResultSet<StoredMessageBatch>> getMessageBatches(MessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		MessageBatchesIteratorProvider provider =
				new MessageBatchesIteratorProvider("get messages batches filtered by " + filter, filter,
						getOperators(), book, composingService, selectQueryExecutor,
						composeReadAttrs(filter.getFetchParameters()));
		return provider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
	}

	public CompletableFuture<CradleResultSet<StoredGroupedMessageBatch>> getGroupedMessageBatches(GroupedMessageFilter filter,
			BookInfo book)
			throws CradleStorageException
	{
		GroupedMessageIteratorProvider provider =
				new GroupedMessageIteratorProvider("get messages batches filtered by " + filter, filter,
						getOperators(), book, composingService, selectQueryExecutor,
						composeReadAttrs(filter.getFetchParameters()), filter.getOrder());
		return provider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
	}

	public CompletableFuture<CradleResultSet<StoredMessage>> getMessages(MessageFilter filter, BookInfo book)
			throws CradleStorageException
	{
		MessagesIteratorProvider provider =
				new MessagesIteratorProvider("get messages filtered by " + filter, filter,
						getOperators(), book, composingService, selectQueryExecutor,
						composeReadAttrs(filter.getFetchParameters()));
		return provider.nextIterator()
				.thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
	}

	private CompletableFuture<Row> getNearestTimeAndSequenceBefore(BookInfo bookInfo, PageInfo page,
			MessageBatchOperator mbOperator, String sessionAlias, String direction, LocalDate messageDate,
			LocalTime messageTime, long sequence, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		String queryInfo = format("get nearest time and sequence before %s for page '%s'",
				TimeUtils.toInstant(messageDate, messageTime), page.getId().getName());
		return selectQueryExecutor.executeSingleRowResultQuery(
						() -> mbOperator.getNearestTimeAndSequenceBefore(page.getId().getBookId().getName(), page.getId().getName(), sessionAlias,
								direction, messageDate, messageTime, sequence, readAttrs), Function.identity(), queryInfo)
				.thenComposeAsync(row ->
				{
					if (row != null)
						return CompletableFuture.completedFuture(row);

					return CompletableFuture.completedFuture(null);
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
			return CompletableFuture.failedFuture(e);
		}

		LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
		CassandraOperators operators = getOperators();
		MessageBatchEntityConverter mbEntityConverter = operators.getMessageBatchEntityConverter();
		MessageBatchOperator mbOperator = operators.getMessageBatchOperator();

		return getNearestTimeAndSequenceBefore(bookInfo, bookInfo.getPage(pageId), mbOperator, id.getSessionAlias(),
				id.getDirection().getLabel(), ldt.toLocalDate(), ldt.toLocalTime(), id.getSequence(), readAttrs)
				.thenComposeAsync(row ->
				{
					if (row == null)
					{
						logger.debug("No message batches found by id '{}'", id);
						return CompletableFuture.completedFuture(null);
					}
					return selectQueryExecutor.executeSingleRowResultQuery(
									() -> mbOperator.get(pageId.getBookId().getName(), pageId.getName(), id.getSessionAlias(),
											id.getDirection().getLabel(), ldt.toLocalDate(),
											row.getLocalTime(FIELD_FIRST_MESSAGE_TIME), row.getLong(FIELD_SEQUENCE), readAttrs),
									mbEntityConverter::getEntity,
									format("get message batch for message with id '%s'", id))
							.thenApplyAsync(entity ->
							{
								if (entity == null)
									return null;
								StoredMessageBatch batch = mapMessageBatchEntity(pageId, entity);
								logger.debug("Message batch with id '{}' found for message with id '{}'",
										batch.getId(), id);
								return batch;
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

	public CompletableFuture<PageGroupEntity> storePageGroup (GroupedMessageBatchEntity groupedMessageBatchEntity) {
		CassandraOperators operators = getOperators();
		PageGroupEntity pageGroupEntity = new PageGroupEntity(
				groupedMessageBatchEntity.getBook(),
				groupedMessageBatchEntity.getPage(),
				groupedMessageBatchEntity.getGroup());

		if (operators.getPageGroupCache().contains(pageGroupEntity)) {
			logger.debug("Skipped writing group '{}' for page '{}'", pageGroupEntity.getGroup(), pageGroupEntity.getPage());
			return CompletableFuture.completedFuture(null);
		}

		logger.debug("Writing group '{}' for page '{}'", pageGroupEntity.getGroup(), pageGroupEntity.getPage());
		return operators.getPageGroupsOperator().write(pageGroupEntity, writeAttrs)
				.whenComplete((result, e) -> {
					if (e == null) {
						operators.getPageGroupCache().store(pageGroupEntity);
					}
				});
	}

	public CompletableFuture<GroupEntity> storeGroup (GroupedMessageBatchEntity groupedMessageBatchEntity) {
		CassandraOperators operators = getOperators();
		GroupEntity groupEntity = new GroupEntity(groupedMessageBatchEntity.getBook(), groupedMessageBatchEntity.getGroup());

		if (operators.getGroupCache().contains(groupEntity)) {
			logger.debug("Skipped writing group '{}'", groupEntity.getGroup());
			return CompletableFuture.completedFuture(null);
		}

		logger.debug("Writing group '{}'", groupEntity.getGroup());
		return operators.getGroupsOperator().write(groupEntity, writeAttrs)
				.whenComplete((result, e) -> {
					if (e == null) {
						operators.getGroupCache().store(groupEntity);
					}
				});
	}

	public CompletableFuture<PageSessionEntity> storePageSession(MessageBatchToStore batch, PageId pageId)
	{
		StoredMessageId batchId = batch.getId();
		CassandraOperators operators = getOperators();
		CachedPageSession cachedPageSession = new CachedPageSession(pageId.toString(),
				batchId.getSessionAlias(), batchId.getDirection().getLabel());
		if (!operators.getPageSessionsCache().store(cachedPageSession))
		{
			logger.debug("Skipped writing page/session of message batch '{}'", batchId);
			return CompletableFuture.completedFuture(null);
		}

		logger.debug("Writing page/session of batch '{}'", batchId);

		return operators.getPageSessionsOperator().write(new PageSessionEntity(batchId, pageId), writeAttrs);
	}

	public CompletableFuture<SessionEntity> storeSession(MessageBatchToStore batch)
	{
		StoredMessageId batchId = batch.getId();
		BookId bookId = batchId.getBookId();
		CassandraOperators operators = getOperators();
		CachedSession cachedSession = new CachedSession(bookId.toString(), batch.getSessionAlias());
		if (!operators.getSessionsCache().store(cachedSession))
		{
			logger.debug("Skipped writing book/session of message batch '{}'", batchId);
			return CompletableFuture.completedFuture(null);
		}
		logger.debug("Writing book/session of batch '{}'", batchId);

		return operators.getSessionsOperator().write(new SessionEntity(bookId.toString(), batch.getSessionAlias()), writeAttrs);
	}

	public CompletableFuture<Void> storeMessageBatch(MessageBatchToStore batch, PageId pageId) {
		BookId bookId = pageId.getBookId();
		MessageBatchOperator mbOperator = getOperators().getMessageBatchOperator();

		return CompletableFuture.supplyAsync(() -> {
			try {
				return MessageBatchEntityUtils.toSerializedEntity(batch, pageId, settings.getMaxUncompressedMessageBatchSize());
			} catch (Exception e) {
				throw new CompletionException(e);
			}
		}).thenComposeAsync(serializedEntity -> {
			MessageBatchEntity entity = serializedEntity.getEntity();
			List<SerializedEntityMetadata> meta = serializedEntity.getSerializedEntityData().getSerializedEntityMetadata();

			return mbOperator.write(entity, writeAttrs)
					.thenRunAsync(() -> messageStatisticsCollector.updateMessageBatchStatistics(bookId,
																								entity.getPage(),
																								entity.getSessionAlias(),
																								entity.getDirection(),
																								meta), composingService)
					.thenRunAsync(() -> sessionStatisticsCollector.updateSessionStatistics(bookId,
																							entity.getPage(),
																							SessionRecordType.SESSION,
																							entity.getSessionAlias(),
																							meta), composingService)
					.thenRunAsync(() -> updateMessageWriteMetrics(entity, bookId), composingService);
		});
	}

	public CompletableFuture<Void> storeGroupedMessageBatch(GroupedMessageBatchToStore batchToStore, PageId pageId) {
		BookId bookId = pageId.getBookId();
		GroupedMessageBatchOperator gmbOperator = getOperators().getGroupedMessageBatchOperator();

		return CompletableFuture.supplyAsync(() -> {
			try {
				return GroupedMessageEntityUtils.toSerializedEntity(batchToStore, pageId, settings.getMaxUncompressedMessageBatchSize());
			} catch (Exception e) {
				throw new CompletionException(e);
			}
		}).thenComposeAsync(serializedEntity -> {
			GroupedMessageBatchEntity entity = serializedEntity.getEntity();
			List<SerializedEntityMetadata> meta = serializedEntity.getSerializedEntityData().getSerializedEntityMetadata();

			gmbOperator.write(entity, writeAttrs);

			return gmbOperator.write(entity, writeAttrs)
					.thenRunAsync(() -> storePageGroup(entity))
					.thenRunAsync(() -> storeGroup(entity))
					.thenRunAsync(() -> messageStatisticsCollector.updateMessageBatchStatistics(bookId,
																								pageId.getName(),
																								entity.getGroup(),
																								"",
																								meta), composingService)
					.thenRunAsync(() -> sessionStatisticsCollector.updateSessionStatistics(bookId,
																							pageId.getName(),
																							SessionRecordType.SESSION_GROUP,
																							entity.getGroup(),
																							meta), composingService)
					.thenRunAsync(() -> updateMessageWriteMetrics(entity, bookId), composingService);
		});
	}

	public long getBoundarySequence(String sessionAlias, Direction direction, BookInfo book, boolean first)
			throws CradleStorageException
	{
		MessageBatchOperator mbOp = getOperators().getMessageBatchOperator();
		PageInfo currentPage = first ? book.getFirstPage() : book.getLastPage();
		Row row = null;

		while (row == null && currentPage != null)
		{
			String page = currentPage.getId().getName();
			String bookName = book.getId().getName();
			String queryInfo = format("get %s sequence for book '%s' page '%s', session alias '%s', " +
					"direction '%s'", (first ? "first" : "last"), bookName, page, sessionAlias, direction);
			try
			{
				row = selectQueryExecutor.executeSingleRowResultQuery(
						() -> first ? mbOp.getFirstSequence(bookName, page, sessionAlias, direction.getLabel(), readAttrs)
								: mbOp.getLastSequence(bookName, page, sessionAlias, direction.getLabel(), readAttrs),
						Function.identity(), queryInfo).get();
			}
			catch (InterruptedException | ExecutionException e)
			{
				throw new CradleStorageException("Error occurs while " + queryInfo, e);
			}

			if (row == null)
				currentPage = first ? book.getNextPage(currentPage.getStarted())
						: book.getPreviousPage(currentPage.getStarted());
		}
		if (row == null)
		{
			logger.debug("There is no messages yet in book '{}' with session alias '{}' and direction '{}'",
					book.getId(), sessionAlias, direction);
			return EMPTY_MESSAGE_INDEX;
		}

		return row.getLong(first ? FIELD_SEQUENCE : FIELD_LAST_SEQUENCE);
	}
}