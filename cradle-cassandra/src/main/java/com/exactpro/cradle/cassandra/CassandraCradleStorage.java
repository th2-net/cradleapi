/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.intervals.CassandraIntervalsWorker;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalSupplies;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.iterators.*;
import com.exactpro.cradle.cassandra.retries.PageSizeAdjustingPolicy;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.cassandra.retries.RetryingSelectExecutor;
import com.exactpro.cradle.cassandra.retries.SelectExecutionPolicy;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.*;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	public static final long EMPTY_MESSAGE_INDEX = -1L;
	private final Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
	public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	private final CassandraSemaphore semaphore;
	private final CradleObjectsFactory objectsFactory;

	private CassandraOperators ops;

	private UUID instanceUuid;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	private final int resultPageSize;
	private SelectExecutionPolicy selectExecutionPolicy;

	private QueryExecutor exec;
	private RetryingSelectExecutor selectExecutor;
	private PagingSupplies pagingSupplies;

	private IntervalsWorker intervalsWorker;

	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		CassandraConnectionSettings conSettings = connection.getSettings();

		this.connection = connection;
		this.settings = settings;
		this.semaphore = new CassandraSemaphore(conSettings.getMaxParallelQueries());
		this.objectsFactory =
				new CradleObjectsFactory(settings.getMaxMessageBatchSize(), settings.getMaxTestEventBatchSize());
		this.resultPageSize = conSettings.getResultPageSize();

		this.selectExecutionPolicy = conSettings.getSelectExecutionPolicy();
		if (this.selectExecutionPolicy == null)
			this.selectExecutionPolicy = new PageSizeAdjustingPolicy(resultPageSize == 0 ? 5000 : resultPageSize, 2);
	}


	public UUID getInstanceUuid()
	{
		return instanceUuid;
	}


	@Override
	protected String doInit(String instanceName, boolean prepareStorage) throws CradleStorageException
	{
		logger.info("Connecting to Cassandra...");
		try
		{
			connection.start();
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Could not open Cassandra connection", e);
		}

		try
		{
			CqlSession session = connection.getSession();
			exec = new QueryExecutor(session,
					settings.getTimeout(), settings.getWriteConsistencyLevel(), settings.getReadConsistencyLevel());
			selectExecutor = new RetryingSelectExecutor(session, selectExecutionPolicy);
			pagingSupplies = new PagingSupplies(session, selectExecutionPolicy);

			if (prepareStorage)
			{
				logger.info("Creating/updating schema...");
				createTables();
				logger.info("All needed tables created");
			}
			else
				logger.info("Schema creation/update skipped");

			instanceUuid = getInstanceId(instanceName);
			CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(session).build();
			ops = createOperators(dataMapper, settings);
			Duration timeout = Duration.ofMillis(settings.getTimeout());
			writeAttrs = builder -> builder
					.setConsistencyLevel(settings.getWriteConsistencyLevel())
					.setTimeout(timeout);
			readAttrs = builder -> builder
					.setConsistencyLevel(settings.getReadConsistencyLevel())
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			strictReadAttrs = builder -> builder
					.setConsistencyLevel(ConsistencyLevel.ALL)
					.setTimeout(timeout)
					.setPageSize(resultPageSize);

			IntervalSupplies intervalSupplies =
					new IntervalSupplies(ops.getIntervalOperator(), ops.getIntervalConverter(), pagingSupplies);
			intervalsWorker =
					new CassandraIntervalsWorker(semaphore, instanceUuid, writeAttrs, readAttrs, intervalSupplies);
			return instanceUuid.toString();
		}
		catch (IOException e)
		{
			throw new CradleStorageException("Could not initialize storage", e);
		}
	}

	@Override
	protected void doDispose()
	{
		logger.info("Disconnecting from Cassandra...");
		try
		{
			connection.stop();
		}
		catch (Exception e)
		{
			logger.error("Error while closing Cassandra connection", e);
		}
	}


	@Override
	protected void doStoreMessageBatch(StoredMessageBatch batch) throws IOException
	{
		try
		{
			doStoreMessageBatchAsync(batch).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch)
	{
		return writeMessage(batch, true);
	}

	@Override
	protected void doStoreTimeMessage(StoredMessage message) throws IOException
	{
		try
		{
			doStoreTimeMessageAsync(message).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing time/message data for message "+message.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTimeMessageAsync(StoredMessage message)
	{
		CompletableFuture<TimeMessageEntity > future = new AsyncOperator<TimeMessageEntity>(semaphore)
				.getFuture(() -> {
					TimeMessageEntity timeEntity = new TimeMessageEntity(message, instanceUuid);

					logger.trace("Executing time/message storing query for message {}", message.getId());
					return ops.getTimeMessageOperator().writeMessage(timeEntity, writeAttrs);
				});
		return future.thenAccept(e -> {});
	}

	@Override
	protected void doStoreProcessedMessageBatch(StoredMessageBatch batch) throws IOException
	{
		try
		{
			doStoreProcessedMessageBatchAsync(batch).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing processed message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreProcessedMessageBatchAsync(StoredMessageBatch batch)
	{
		return writeMessage(batch, false);
	}


	@Override
	protected void doStoreTestEvent(StoredTestEvent event) throws IOException
	{
		try
		{
			doStoreTestEventAsync(event).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing test event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(StoredTestEvent event)
	{
		List<CompletableFuture<Void>> futures = new ArrayList<>();
		try
		{
			futures.add(storeTimeEvent(new DetailedTestEventEntity(event, instanceUuid)));
			futures.add(storeDateTime(new DateTimeEventEntity(event, instanceUuid)));
			futures.add(storeChildrenDates(new ChildrenDatesEventEntity(event, instanceUuid)));
		}
		catch (IOException e)
		{
			CompletableFuture<Void> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			futures.add(error);
		}

		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
	}

	@Override
	protected void doUpdateParentTestEvents(StoredTestEvent event) throws IOException
	{
		if (event.isSuccess())
			return;

		try
		{
			doUpdateParentTestEventsAsync(event).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while updating parents of "+event.getId()+" test event", e);
		}
	}

	@Override
	protected CompletableFuture<Void> doUpdateParentTestEventsAsync(StoredTestEvent event)
	{
		if (event.isSuccess())
			return CompletableFuture.completedFuture(null);

		return failEventAndParents(event.getParentId());
	}

	@Override
	protected StoredMessage doGetMessage(StoredMessageId id) throws IOException
	{
		try
		{
			return doGetMessageAsync(id).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message "+id, e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id)
	{
		return readMessage(id, true);
	}

	@Override
	protected Collection<StoredMessage> doGetMessageBatch(StoredMessageId id) throws IOException
	{
		try
		{
			return doGetMessageBatchAsync(id).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message batch "+id, e);
		}
	}

	@Override
	protected CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id)
	{
		CompletableFuture<DetailedMessageBatchEntity> entityFuture = readMessageBatchEntity(id, true);
		return entityFuture.thenApply(entity -> {
			if (entity == null)
				return null;

			try
			{
				return MessageUtils.bytesToMessages(entity.getContent(), entity.isCompressed());
			}
			catch (IOException e)
			{
				throw new CompletionException("Error while reading message batch", e);
			}
		});
	}

	@Override
	protected StoredMessage doGetProcessedMessage(StoredMessageId id) throws IOException
	{
		try
		{
			return doGetProcessedMessageAsync(id).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting processed message "+id, e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessage> doGetProcessedMessageAsync(StoredMessageId id)
	{
		return readMessage(id, false);
	}

	@Override
	protected long doGetFirstMessageIndex(String streamName, Direction direction) throws IOException
	{
		return getFirstIndex(ops.getMessageBatchOperator(), streamName, direction);
	}

	@Override
	protected long doGetLastMessageIndex(String streamName, Direction direction) throws IOException
	{
		return getLastIndex(ops.getMessageBatchOperator(), streamName, direction);
	}

	@Override
	protected long doGetFirstProcessedMessageIndex(String streamName, Direction direction) throws IOException
	{
		return getFirstIndex(ops.getProcessedMessageBatchOperator(), streamName, direction);
	}

	@Override
	protected long doGetLastProcessedMessageIndex(String streamName, Direction direction) throws IOException
	{
		return getLastIndex(ops.getProcessedMessageBatchOperator(), streamName, direction);
	}

	@Override
	protected StoredMessageId doGetNearestMessageId(String streamName, Direction direction, Instant timestamp,
			TimeRelation timeRelation) throws IOException
	{
		try
		{
			return doGetNearestMessageIdAsync(streamName, direction, timestamp, timeRelation).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting nearest message ID", e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessageId> doGetNearestMessageIdAsync(String streamName, Direction direction,
			Instant timestamp, TimeRelation timeRelation)
	{
		CompletableFuture<TimeMessageEntity> timeMessageEntityFuture =
				readTimeMessageEntity(streamName, direction, timestamp, timeRelation);
		return timeMessageEntityFuture.thenCompose(entity ->
		{
			if (entity == null)
				return CompletableFuture.completedFuture(null);

			return CompletableFuture.completedFuture(entity.createMessageId());
		});
	}

	private CompletableFuture<TimeMessageEntity> readTimeMessageEntity(String streamName, Direction direction,
			Instant timestamp, TimeRelation timeRelation)
	{
		LocalDateTime messageDateTime = LocalDateTime.ofInstant(timestamp, TIMEZONE_OFFSET);

		return timeRelation == TimeRelation.BEFORE
				? ops.getTimeMessageOperator()
						.getNearestMessageBefore(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs)
				: ops.getTimeMessageOperator()
						.getNearestMessageAfter(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs);
	}

	@Override
	protected StoredTestEventWrapper doGetTestEvent(StoredTestEventId id) throws IOException
	{
		try
		{
			return doGetTestEventAsync(id).get();
		}
		catch (Exception e)
		{
			throw new IOException("Could not get test event", e);
		}
	}

	@Override
	protected CompletableFuture<StoredTestEventWrapper> doGetTestEventAsync(StoredTestEventId id)
	{
		CompletableFuture<DateTimeEventEntity> future = new AsyncOperator<DateTimeEventEntity>(semaphore)
				.getFuture(() -> ops.getTestEventOperator().get(instanceUuid, id.toString(), readAttrs));
		return future.thenCompose(dtEntity ->
		{
			if (dtEntity == null)
				return CompletableFuture.completedFuture(null);

			return new AsyncOperator<StoredTestEventWrapper>(semaphore).getFuture(() -> ops.getTimeTestEventOperator()
					.get(instanceUuid, dtEntity.getStartDate(), dtEntity.getStartTime(), id.toString(), readAttrs)
					.thenApply(entity ->
					{
						try
						{
							return entity != null ? entity.toStoredTestEventWrapper() : null;
						}
						catch (Exception error)
						{
							throw new CompletionException("Error while converting data into test event", error);
						}
					}));
		});
	}

	@Override
	public IntervalsWorker getIntervalsWorker() { return intervalsWorker; }


	@Override
	protected Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException
	{
		try
		{
			return doGetMessagesAsync(filter).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting messages filtered by "+filter, e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter)
	{
		String queryInfo = "get messages filtered by " + filter;
		return doGetDetailedMessageBatchEntities(filter, queryInfo)
				.thenApply(it -> new MessagesIteratorAdapter(filter, it, pagingSupplies,
						ops.getMessageBatchConverter(), queryInfo));
	}


	@Override
	protected Iterable<StoredMessageBatch> doGetMessagesBatches(StoredMessageFilter filter) throws IOException
	{
		try
		{
			return doGetMessagesBatchesAsync(filter).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting messages filtered by "+filter, e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter)
	{
		String queryInfo = "get message batches filtered by "+filter;
		return doGetDetailedMessageBatchEntities(filter, queryInfo)
				.thenApply(it -> new StoredMessageBatchAdapter(it, pagingSupplies, ops.getMessageBatchConverter(),
						queryInfo, objectsFactory, filter == null ? 0 : filter.getLimit()));
	}

	private CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> doGetDetailedMessageBatchEntities(
			StoredMessageFilter filter, String queryInfo)
	{
		MessageBatchOperator mbOp = ops.getMessageBatchOperator();
		TimeMessageOperator tmOp = ops.getTimeMessageOperator();
		return new AsyncOperator<MappedAsyncPagingIterable<DetailedMessageBatchEntity>>(semaphore)
						.getFuture(() -> selectExecutor
								.executeQuery(() -> mbOp.filterMessages(instanceUuid, filter, mbOp, tmOp, readAttrs),
										ops.getMessageBatchConverter(), queryInfo));
	}

	@Override
	protected Iterable<StoredTestEventWrapper> doGetRootTestEvents(Instant from, Instant to)
			throws CradleStorageException, IOException
	{
		try
		{
			return doGetRootTestEventsAsync(from, to).get();
		}
		catch (CradleStorageException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting root test events", e);
		}
	}

	@Override
	protected Iterable<StoredTestEventMetadata> doGetRootTestEventsMetadata(Instant from, Instant to)
			throws CradleStorageException, IOException
	{
		try
		{
			return doGetRootTestEventsMetadataAsync(from, to).get();
		}
		catch (CradleStorageException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting root test events' metadata", e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetRootTestEventsAsync(Instant from, Instant to)
			throws CradleStorageException
	{
		return doGetTestEventsAsync(null, from, to);
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetRootTestEventsMetadataAsync(Instant from,
			Instant to) throws CradleStorageException
	{
		return doGetTestEventsMetadataAsync(null, from, to);
	}


	@Override
	protected Iterable<StoredTestEventWrapper> doGetTestEvents(StoredTestEventId parentId, Instant from, Instant to)
			throws CradleStorageException, IOException
	{
		try
		{
			return doGetTestEventsAsync(parentId, from, to).get();
		}
		catch (CradleStorageException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting child test events", e);
		}
	}

	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEventsMetadata(StoredTestEventId parentId, Instant from,
			Instant to) throws CradleStorageException, IOException
	{
		try
		{
			return doGetTestEventsMetadataAsync(parentId, from, to).get();
		}
		catch (CradleStorageException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting child test events' metadata", e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetTestEventsAsync(StoredTestEventId parentId,
			Instant from, Instant to) throws CradleStorageException
	{
		TestEventsQueryParams params = new TestEventsQueryParams(parentId, from, to);
		String queryInfo = String.format("get %s from range %s..%s",
				parentId == null ? "root" : "child test events of '" + parentId + "'", from, to);

		return new AsyncOperator<MappedAsyncPagingIterable<TestEventEntity>>(semaphore)
				.getFuture(() -> selectExecutor.executeQuery(() ->
								ops.getTimeTestEventOperator().getTestEvents(
										instanceUuid,
										params.getFromDate(),
										params.getFromTime(),
										null,
										params.getToTime(),
										params.getParentId(),
										readAttrs),
						ops.getTestEventConverter(), queryInfo))
				.thenApply(r -> new TestEventDataIteratorAdapter(r, pagingSupplies, ops.getTestEventConverter(),
						queryInfo));
	}

	@Override
	protected Iterable<StoredTestEventWrapper> doGetTestEventsFromId(StoredTestEventId fromId, Instant to, Order order) throws ExecutionException, InterruptedException {
		return doGetTestEventsFromIdAsync(fromId, to, order).get();
	}

	@Override
	protected Iterable<StoredTestEventWrapper> doGetTestEventsFromId(StoredTestEventId parentId, StoredTestEventId fromId, Instant to) throws ExecutionException, InterruptedException {
		return doGetTestEventsFromIdAsync(parentId, fromId, to).get();
	}

	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEventsFromIdMetadata(StoredTestEventId fromId, Instant to, Order order) throws ExecutionException, InterruptedException {
		return doGetTestEventsFromIdMetadataAsync(fromId, to, order).get();
	}

	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEventsFromIdMetadata(StoredTestEventId parentId, StoredTestEventId fromId, Instant to) throws CradleStorageException, ExecutionException, InterruptedException {
		return doGetTestEventsFromIdMetadataAsync(parentId, fromId, to).get();
	}


	private<T> CompletableFuture<T> getEventTimestampAndThenCompose(StoredTestEventId fromId, Callback<Instant, CompletableFuture<T>> callback) {

		return new AsyncOperator<DateTimeEventEntity>(semaphore)
				.getFuture(() -> ops.getTestEventOperator().get(instanceUuid, fromId.getId(), readAttrs))
				.thenCompose(eventDateTime -> {

							if (eventDateTime == null)
								return CompletableFuture.completedFuture(null);
							Instant from = eventDateTime.getStartTimestamp();

							try {
								return callback.call(from);
							} catch (Exception e) {
								throw new CompletionException(e);
							}
						});
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetTestEventsFromIdAsync(StoredTestEventId fromId, Instant to, Order order) {

		return getEventTimestampAndThenCompose(fromId, from -> {

				TestEventsQueryParams params = new TestEventsQueryParams(fromId, from, to, order);
				String queryInfo = String.format("get test events starting with id %s from range %s..%s", fromId, from, to);

				return new AsyncOperator<MappedAsyncPagingIterable<TestEventEntity>>(semaphore)
						.getFuture(() -> selectExecutor.executeQuery(() ->
										ops.getTimeTestEventOperator().getTestEvents(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												order,
												readAttrs),
								ops.getTestEventConverter(), queryInfo))
						.thenApply(entity -> new TestEventDataIteratorAdapter(entity, pagingSupplies,
								ops.getTestEventConverter(), queryInfo));

		});
	}


	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetTestEventsFromIdAsync(StoredTestEventId parentId, StoredTestEventId fromId, Instant to) {

		return getEventTimestampAndThenCompose(fromId, from -> {

				TestEventsQueryParams params = new TestEventsQueryParams(parentId, fromId, from, to, null);
				String queryInfo = String.format("get test events starting with id %s and parentId %s from range %s..%s", fromId, parentId, from, to);

				return new AsyncOperator<MappedAsyncPagingIterable<TestEventEntity>>(semaphore)
						.getFuture(() -> selectExecutor.executeQuery(() ->
										ops.getTimeTestEventOperator().getTestEvents(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												params.getParentId(),
												readAttrs),
								ops.getTestEventConverter(), queryInfo))
						.thenApply(entity -> new TestEventDataIteratorAdapter(entity, pagingSupplies,
								ops.getTestEventConverter(), queryInfo));
		});
	}


	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsFromIdMetadataAsync(StoredTestEventId fromId, Instant to, Order order) {

		return getEventTimestampAndThenCompose(fromId, from -> {

				TestEventsQueryParams params = new TestEventsQueryParams(fromId, from, to);
				String queryInfo = String.format("get test events' metadata starting with id %s from range %s..%s", fromId, from, to);

				return new AsyncOperator<MappedAsyncPagingIterable<TestEventMetadataEntity>>(semaphore)
						.getFuture(() -> selectExecutor.executeQuery(() ->
										ops.getTimeTestEventOperator().getTestEventsMetadata(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												order,
												readAttrs),
								ops.getTestEventMetadataConverter(), queryInfo))
						.thenApply(r -> new TestEventMetadataIteratorAdapter(r, pagingSupplies,
								ops.getTestEventMetadataConverter(),
								queryInfo));
		});
	}


	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsFromIdMetadataAsync(StoredTestEventId parentId, StoredTestEventId fromId, Instant to) {

		return getEventTimestampAndThenCompose(fromId, from -> {

				TestEventsQueryParams params = new TestEventsQueryParams(parentId, fromId, from, to, null);
				String queryInfo = String.format("get test events' metadata starting with id %s and parentId %s from range %s..%s", fromId, parentId, from, to);

				return new AsyncOperator<MappedAsyncPagingIterable<TestEventMetadataEntity>>(semaphore)
						.getFuture(() -> selectExecutor.executeQuery(() ->
										ops.getTimeTestEventOperator().getTestEventsMetadata(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												params.getParentId(),
												readAttrs),
								ops.getTestEventMetadataConverter(), queryInfo))
						.thenApply(r -> new TestEventMetadataIteratorAdapter(r, pagingSupplies,
								ops.getTestEventMetadataConverter(),
								queryInfo));
		});
	}


	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsMetadataAsync(
			StoredTestEventId parentId, Instant from, Instant to) throws CradleStorageException
	{
		TestEventsQueryParams params = new TestEventsQueryParams(parentId, from, to);
		String queryInfo = String.format("get %s from range %s..%s",
				parentId == null ? "root" : "child test events' metadata of '" + parentId + "'", from, to);

		return new AsyncOperator<MappedAsyncPagingIterable<TestEventMetadataEntity>>(semaphore)
				.getFuture(() -> selectExecutor.executeQuery(() ->
						ops.getTimeTestEventOperator().getTestEventsMetadata(
								instanceUuid,
								params.getFromDate(),
								params.getFromTime(),
								null,
								params.getToTime(),
								params.getParentId(),
								readAttrs),
						ops.getTestEventMetadataConverter(), queryInfo))
				.thenApply(r -> new TestEventMetadataIteratorAdapter(r, pagingSupplies,
						ops.getTestEventMetadataConverter(),
						queryInfo));
	}


	@Override
	protected Iterable<StoredTestEventWrapper> doGetTestEvents(Instant from, Instant to)
			throws CradleStorageException, IOException
	{
		try
		{
			return doGetTestEventsAsync(from, to).get();
		}
		catch (CradleStorageException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting test events", e);
		}
	}

	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEventsMetadata(Instant from, Instant to)
			throws CradleStorageException, IOException
	{
		try
		{
			return doGetTestEventsMetadataAsync(from, to).get();
		}
		catch (CradleStorageException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting test events' metadata", e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetTestEventsAsync(Instant from, Instant to)
			throws CradleStorageException
	{
		TestEventsQueryParams params = new TestEventsQueryParams(from, to);
		String queryInfo = String.format("get test events from range %s..%s", from, to);

		return new AsyncOperator<MappedAsyncPagingIterable<TestEventEntity>>(semaphore)
				.getFuture(() -> selectExecutor.executeQuery(
						() -> ops.getTimeTestEventOperator().getTestEvents(instanceUuid,
								params.getFromDate(), params.getFromTime(), params.getToTime(), readAttrs),
						ops.getTestEventConverter(), queryInfo))
				.thenApply(entity -> new TestEventDataIteratorAdapter(entity, pagingSupplies,
						ops.getTestEventConverter(), queryInfo));
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsMetadataAsync(Instant from,
			Instant to) throws CradleStorageException
	{
		TestEventsQueryParams params = new TestEventsQueryParams(from, to);
		String queryInfo = String.format("get test events' metadata from range %s..%s", from, to);

		return new AsyncOperator<MappedAsyncPagingIterable<TestEventMetadataEntity>>(semaphore)
				.getFuture(() -> selectExecutor.executeQuery(() ->
						ops.getTimeTestEventOperator().getTestEventsMetadata(
								instanceUuid,
								params.getFromDate(),
								params.getFromTime(),
								null,
								params.getToTime(),
								Order.DIRECT,
								readAttrs),
						ops.getTestEventMetadataConverter(), queryInfo))
				.thenApply(entity -> new TestEventMetadataIteratorAdapter(entity, pagingSupplies,
						ops.getTestEventMetadataConverter(), queryInfo));
	}


	@Override
	protected Collection<String> doGetStreams()
	{
		List<String> result = new ArrayList<>();
		for (StreamEntity entity : ops.getMessageBatchOperator().getStreams(readAttrs))
		{
			if (instanceUuid.equals(entity.getInstanceId()))
				result.add(entity.getStreamName());
		}
		result.sort(null);
		return result;
	}

	@Override
	protected Collection<Instant> doGetRootTestEventsDates()
	{
		return doGetTestEventsDates(new StoredTestEventId(ROOT_EVENT_PARENT_ID));
	}

	@Override
	protected Collection<Instant> doGetTestEventsDates(StoredTestEventId parentId)
	{
		ResultSet rs = ops.getTestEventChildrenDatesOperator().get(instanceUuid, parentId.toString(), readAttrs);
		Collection<Instant> result = new TreeSet<>();
		rs.iterator().forEachRemaining(row ->
		{
			LocalDate startDate;
			if (row != null && (startDate = row.getLocalDate(START_DATE)) != null)
				result.add(startDate.atStartOfDay(TIMEZONE_OFFSET).toInstant());
		});
		return result;
	}

	@Override
	public CradleObjectsFactory getObjectsFactory()
	{
		return objectsFactory;
	}


	protected void createTables() throws IOException
	{
		new TablesCreator(exec, settings).createAll();
	}

	protected CassandraOperators createOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		return new CassandraOperators(dataMapper, settings);
	}

	protected CassandraStorageSettings getSettings()
	{
		return settings;
	}

	public Function<BoundStatementBuilder, BoundStatementBuilder> getWriteAttrs()
	{
		return writeAttrs;
	}

	public Function<BoundStatementBuilder, BoundStatementBuilder> getReadAttrs()
	{
		return readAttrs;
	}

	public Function<BoundStatementBuilder, BoundStatementBuilder> getStrictReadAttrs()
	{
		return strictReadAttrs;
	}


	protected UUID getInstanceId(String instanceName) throws IOException
	{
		UUID id;
		Select selectFrom = selectFrom(settings.getKeyspace(), INSTANCES_TABLE_DEFAULT_NAME)
				.column(ID)
				.whereColumn(NAME).isEqualTo(literal(instanceName));

		Row resultRow = exec.executeQuery(selectFrom.asCql(), false).one();
		if (resultRow != null)
			id = resultRow.get(ID, GenericType.UUID);
		else
		{
			id = UUID.randomUUID();
			Insert insert = insertInto(settings.getKeyspace(), INSTANCES_TABLE_DEFAULT_NAME)
					.value(ID, literal(id))
					.value(NAME, literal(instanceName))
					.ifNotExists();
			exec.executeQuery(insert.asCql(), true);
		}

		return id;
	}


	protected QueryExecutor getQueryExecutor()
	{
		return exec;
	}

	protected CassandraSemaphore getSemaphore()
	{
		return semaphore;
	}


	private CompletableFuture<Void> writeMessage(StoredMessageBatch batch, boolean rawMessage)
	{
		CompletableFuture<DetailedMessageBatchEntity> future = new AsyncOperator<DetailedMessageBatchEntity>(semaphore)
				.getFuture(() ->
				{
					DetailedMessageBatchEntity entity;
					try
					{
						entity = new DetailedMessageBatchEntity(batch, instanceUuid);
					}
					catch (IOException e)
					{
						CompletableFuture<DetailedMessageBatchEntity> error = new CompletableFuture<>();
						error.completeExceptionally(e);
						return error;
					}

					logger.trace("Executing message batch storing query");
					MessageBatchOperator op =
							rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
					return op.writeMessageBatch(entity, writeAttrs);
				});
		return future.thenAccept(e -> {});
	}

	private CompletableFuture<DetailedMessageBatchEntity> readMessageBatchEntity(StoredMessageId messageId,
			boolean rawMessage)
	{
		MessageBatchOperator op = rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
		return new AsyncOperator<DetailedMessageBatchEntity>(semaphore)
				.getFuture(() -> CassandraMessageUtils.getMessageBatch(messageId, op, instanceUuid, readAttrs));
	}

	private CompletableFuture<StoredMessage> readMessage(StoredMessageId id, boolean rawMessage)
	{
		CompletableFuture<DetailedMessageBatchEntity> entityFuture = readMessageBatchEntity(id, rawMessage);
		return entityFuture.thenApply(entity -> {
			if (entity == null)
				return null;

			try
			{
				return MessageUtils.bytesToOneMessage(entity.getContent(), entity.isCompressed(), id);
			}
			catch (IOException e)
			{
				throw new CompletionException("Error while reading message", e);
			}
		});
	}

	private long getFirstIndex(MessageBatchOperator op, String streamName, Direction direction) throws IOException
	{
		CompletableFuture<Row> future = new AsyncOperator<Row>(semaphore).getFuture(
				() -> op.getFirstIndex(instanceUuid, streamName, direction.getLabel(), readAttrs));
		try
		{
			Row row = future.get();
			return row == null ? EMPTY_MESSAGE_INDEX : row.getLong(MESSAGE_INDEX);
		}
		catch (Exception e)
		{
			throw new IOException(
					"Error while getting index of the first message for stream '" + streamName + " and direction '" +
							direction + "'", e);
		}
	}

	private long getLastIndex(MessageBatchOperator op, String streamName, Direction direction) throws IOException
	{
		CompletableFuture<Row> future = new AsyncOperator<Row>(semaphore).getFuture(
				() -> op.getLastIndex(instanceUuid, streamName, direction.getLabel(), readAttrs));
		try
		{
			Row row = future.get();
			return row == null ? EMPTY_MESSAGE_INDEX : row.getLong(LAST_MESSAGE_INDEX);
		}
		catch (Exception e)
		{
			throw new IOException(
					"Error while getting index of the last message for stream '" + streamName + " and direction '" +
							direction + "'", e);
		}
	}

	protected CompletableFuture<Void> storeDateTime(DateTimeEventEntity entity)
	{
		return new AsyncOperator<Void>(semaphore).getFuture(() -> {
			logger.trace("Executing test event storing query");
			return ops.getTestEventOperator().write(entity, writeAttrs);
		});
	}

	protected CompletableFuture<Void> storeChildrenDates(ChildrenDatesEventEntity entity)
	{
		return new AsyncOperator<Void>(semaphore).getFuture(() ->
		{
			logger.trace("Executing date/child event storing query");
			return ops.getTestEventChildrenDatesOperator().writeTestEventDate(entity, writeAttrs);
		});
	}


	protected CompletableFuture<Void> storeTimeEvent(DetailedTestEventEntity entity)
	{
		return new AsyncOperator<Void>(semaphore).getFuture(() -> {
			logger.trace("Executing time/event storing query");
			return ops.getTimeTestEventOperator().writeTestEvent(entity, writeAttrs);
		});
	}

	@Override
	protected void doUpdateEventStatus(StoredTestEventWrapper event, boolean success) throws IOException
	{
		try
		{
			doUpdateEventStatusAsync(event, success).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while updating status of event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEventWrapper event, boolean success)
	{
		String id = event.getId().toString();
		LocalDateTime ldt = LocalDateTime.ofInstant(event.getStartTimestamp(), TIMEZONE_OFFSET);
		LocalDate ld = ldt.toLocalDate();
		LocalTime lt = ldt.toLocalTime();

		return new AsyncOperator<Void>(semaphore)
				.getFuture(() -> ops.getTimeTestEventOperator()
						.updateStatus(instanceUuid, ld, lt, id, success, writeAttrs));
	}

	protected CompletableFuture<Void> failEventAndParents(StoredTestEventId eventId)
	{
		return getTestEventAsync(eventId)
				.thenComposeAsync(event -> {
					//Invalid event ID or event is already failed, which means that its parents are already updated
					if (event == null || !event.isSuccess())
						return CompletableFuture.completedFuture(null);

					CompletableFuture<Void> update = doUpdateEventStatusAsync(event, false);
					if (event.getParentId() != null)
						return update.thenComposeAsync(u -> failEventAndParents(event.getParentId()));
					return update;
				});
	}

	private interface Callback<T, R> {
		R call(T param) throws Exception;
	}

	private static class TestEventsQueryParams
	{
		private final LocalDateTime fromDateTime, toDateTime;
		private final String parentId;
		private final String fromId;
		private final Order order;

		public TestEventsQueryParams(StoredTestEventId parentId, StoredTestEventId fromId, Instant from, Instant to, Order order)
				throws CradleStorageException
		{
			this.fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET);
			this.toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
			this.parentId = (parentId == null) ? ROOT_EVENT_PARENT_ID : parentId.toString();
			this.fromId = (fromId == null) ? null : fromId.getId();
			this.order = order;

			checkTimeBoundaries(fromDateTime, toDateTime, from, to);
		}

		public TestEventsQueryParams(StoredTestEventId fromId, Instant from, Instant to, Order order)
				throws CradleStorageException
		{
			this (null, fromId, from, to, order);
		}

		public TestEventsQueryParams(StoredTestEventId parentId, Instant from, Instant to)
				throws CradleStorageException
		{
			this (parentId, null, from, to, Order.DIRECT);
		}

		public TestEventsQueryParams(Instant from, Instant to) throws CradleStorageException
		{
			this(null, from, to);
		}

		private void checkTimeBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime, Instant originalFrom,
				Instant originalTo) throws CradleStorageException
		{
			LocalDate fromDate = fromDateTime.toLocalDate(),
					toDate = toDateTime.toLocalDate();
			if (!fromDate.equals(toDate))
				throw new CradleStorageException(
						"Left and right boundaries should be of the same date, but got '" + originalFrom + "' and '" +
								originalTo + "'");
		}

		public String getParentId()
		{
			return parentId;
		}

		public LocalDate getFromDate()
		{
			return fromDateTime.toLocalDate();
		}

		public LocalTime getFromTime()
		{
			return fromDateTime.toLocalTime();
		}

		public LocalTime getToTime()
		{
			return toDateTime.toLocalTime();
		}

		public String getFromId() {
			return fromId;
		}

		public Order getOrder() {
			return order;
		}
	}
}
