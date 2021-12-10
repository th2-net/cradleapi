/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
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
import com.exactpro.cradle.cassandra.dao.messages.converters.TimeMessageConverter;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.dao.testevents.converters.RootTestEventsDatesConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventChildDatesConverter;
import com.exactpro.cradle.cassandra.iterators.*;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.linkers.LinkerSupplies;
import com.exactpro.cradle.cassandra.retries.*;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
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
	private int resultPageSize;
	private SelectExecutionPolicy multiRowResultExecPolicy, singleRowResultExecPolicy;
	
	private QueryExecutor exec;
	private SelectQueryExecutor selectExecutor;
	private CompleteEventsGetter completeEventsGetter;
	private PagingSupplies pagingSupplies;

	private TestEventsMessagesLinker testEventsMessagesLinker;
	private IntervalsWorker intervalsWorker;

	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		CassandraConnectionSettings conSettings = connection.getSettings();
		
		this.connection = connection;
		this.settings = settings;
		this.semaphore = new CassandraSemaphore(conSettings.getMaxParallelQueries());
		this.objectsFactory = new CradleObjectsFactory(settings.getMaxMessageBatchSize(), settings.getMaxTestEventBatchSize());
		this.resultPageSize = conSettings.getResultPageSize();
		
		this.multiRowResultExecPolicy = conSettings.getSelectExecutionPolicy();
		if (this.multiRowResultExecPolicy == null)
			this.multiRowResultExecPolicy = new PageSizeAdjustingPolicy(resultPageSize == 0 ? 5000 : resultPageSize, 2);

		this.singleRowResultExecPolicy = conSettings.getSingleRowResultExecutionPolicy();
		if (this.singleRowResultExecPolicy == null)
			this.singleRowResultExecPolicy = new FixedNumberRetryPolicy(5);
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
			selectExecutor = new SelectQueryExecutor(session, multiRowResultExecPolicy, singleRowResultExecPolicy);
			pagingSupplies = new PagingSupplies(session, multiRowResultExecPolicy);
			
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
			writeAttrs = builder -> builder.setConsistencyLevel(settings.getWriteConsistencyLevel())
					.setTimeout(timeout);
			readAttrs = builder -> builder.setConsistencyLevel(settings.getReadConsistencyLevel())
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			strictReadAttrs = builder -> builder.setConsistencyLevel(ConsistencyLevel.ALL)
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			
			LinkerSupplies supplies = new LinkerSupplies(ops.getTestEventMessagesOperator(), ops.getMessageTestEventOperator(),
					ops.getTestEventMessagesConverter(), ops.getMessageTestEventConverter());
			testEventsMessagesLinker = new CassandraTestEventsMessagesLinker(supplies, instanceUuid, readAttrs, semaphore,
					selectExecutor, pagingSupplies);
			completeEventsGetter = new CompleteEventsGetter(instanceUuid, readAttrs, multiRowResultExecPolicy,
					ops.getTestEventOperator(), ops.getTestEventConverter(), pagingSupplies);
			
			IntervalSupplies intervalSupplies = new IntervalSupplies(ops.getIntervalOperator(), ops.getIntervalConverter(), pagingSupplies);
			intervalsWorker = new CassandraIntervalsWorker(semaphore, instanceUuid, writeAttrs, readAttrs, intervalSupplies);
			return instanceUuid.toString();
		}
		catch (IOException e)
		{
			throw new CradleStorageException("Could not initialize storage", e);
		}
	}

	@Override
	protected void doDispose() throws CradleStorageException
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
		futures.add(storeEvent(event).thenAccept(r -> {}));
		futures.add(storeTimeEvent(event).thenAccept(r -> {}));
		if (event.getParentId() != null)
		{
			futures.add(storeEventInParent(event).thenAccept(r -> {}));
			futures.add(storeEventDateInParent(event).thenAccept(r -> {}));
		}
		else
			futures.add(storeRootEvent(event).thenAccept(r -> {}));

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
	protected void doStoreTestEventMessagesLink(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messageIds) throws IOException
	{
		try
		{
			doStoreTestEventMessagesLinkAsync(eventId, batchId, messageIds).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing link between test event "+eventId+" and "+messageIds.size()+" message(s)", e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTestEventMessagesLinkAsync(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messageIds)
	{
		List<String> messageIdsStrings = messageIds.stream().map(StoredMessageId::toString).collect(toList());
		String eventIdString = eventId.toString();
		return CompletableFuture.allOf(storeMessagesOfTestEvent(eventIdString, messageIdsStrings));
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
		return entityFuture.thenApplyAsync(entity -> {
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
	protected StoredMessageId doGetNearestMessageId(String streamName, Direction direction, Instant timestamp, TimeRelation timeRelation) throws IOException
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
		TimeMessageOperator tmOperator = ops.getTimeMessageOperator();
		TimeMessageConverter converter = ops.getTimeMessageConverter();
		CompletableFuture<TimeMessageEntity> result = timeRelation == TimeRelation.BEFORE
				? selectExecutor.executeSingleRowResultQuery(
						() -> tmOperator.getNearestMessageBefore(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs),
						converter, "getting nearest message time before " + timestamp)

				: selectExecutor.executeSingleRowResultQuery(
						() -> tmOperator.getNearestMessageAfter(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs),
						converter, "getting nearest message time after " + timestamp);

		return result;
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
		CompletableFuture<TestEventEntity> future = new AsyncOperator<TestEventEntity>(semaphore)
				.getFuture(() -> ops.getTestEventOperator().get(instanceUuid, id.toString(), readAttrs));
		return future.thenApply(e -> {
				try
				{
					return e != null ? e.toStoredTestEventWrapper() : null;
				}
				catch (Exception error)
				{
					throw new CompletionException("Could not get test event", error);
				}
			});
	}

	@Override
	protected Iterable<StoredTestEventWrapper> doGetCompleteTestEvents(Set<StoredTestEventId> ids) throws IOException
	{
		try
		{
			return doGetCompleteTestEventsAsync(ids).get();
		}
		catch (Exception e)
		{
			throw new IOException("Could not get test events", e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetCompleteTestEventsAsync(Set<StoredTestEventId> id)
	{
		return new AsyncOperator<Iterable<StoredTestEventWrapper>>(semaphore)
						.getFuture(() -> completeEventsGetter.get(id, "get test events "+id));
	}

	@Override
	public TestEventsMessagesLinker getTestEventsMessagesLinker()
	{
		return testEventsMessagesLinker;
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
		String queryInfo = "getting messages filtered by "+filter;
		return doGetDetailedMessageBatchEntities(filter, queryInfo)
				.thenApply(it -> new MessagesIteratorAdapter(filter, it, pagingSupplies, ops.getMessageBatchConverter(), queryInfo));
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
		String queryInfo = "getting message batches filtered by "+filter;
		return doGetDetailedMessageBatchEntities(filter, queryInfo)
				.thenApply(it -> new StoredMessageBatchAdapter(it, pagingSupplies, ops.getMessageBatchConverter(), queryInfo,
						objectsFactory, filter == null ? 0 : filter.getLimit()));
	}

	private CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> doGetDetailedMessageBatchEntities(
			StoredMessageFilter filter, String queryInfo)
	{
		MessageBatchOperator mbOp = ops.getMessageBatchOperator();
		TimeMessageOperator tmOp = ops.getTimeMessageOperator();
		return new AsyncOperator<MappedAsyncPagingIterable<DetailedMessageBatchEntity>>(semaphore)
				.getFuture(() -> selectExecutor.executeMultiRowResultQuery(
						() -> mbOp.filterMessages(instanceUuid, filter, mbOp, tmOp, readAttrs),
						ops.getMessageBatchConverter(), queryInfo));
	}

	@Override
	protected Iterable<StoredTestEventMetadata> doGetRootTestEvents(Instant from, Instant to, Order order) 
			throws CradleStorageException, IOException
	{
		try
		{
			return doGetRootTestEventsAsync(from, to, order).get();
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
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetRootTestEventsAsync(Instant from, Instant to,
			Order order) throws CradleStorageException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		checkTimeBoundaries(fromDateTime, toDateTime, from, to);

		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();
		
		String queryInfo = "get root test events from range "+from+".."+to+" in "+order+" order";
		RootTestEventOperator op = ops.getRootTestEventOperator();
		CompletableFuture<MappedAsyncPagingIterable<RootTestEventEntity>> future = 
				new AsyncOperator<MappedAsyncPagingIterable<RootTestEventEntity>>(semaphore)
						.getFuture(() -> selectExecutor.executeMultiRowResultQuery(
								() -> order == Order.DIRECT
										? op.getTestEventsDirect(instanceUuid, fromDateTime.toLocalDate(), fromTime,
												toTime, readAttrs)
										: op.getTestEventsReverse(instanceUuid, fromDateTime.toLocalDate(), fromTime,
												toTime, readAttrs),
								ops.getRootTestEventConverter(), queryInfo));
		return future.thenApply(result -> new RootTestEventsMetadataIteratorAdapter(result, pagingSupplies, ops.getRootTestEventConverter(), queryInfo));
	}


	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(StoredTestEventId parentId, Instant from, Instant to, Order order) 
			throws CradleStorageException, IOException
	{
		try
		{
			return doGetTestEventsAsync(parentId, from, to, order).get();
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
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsAsync(StoredTestEventId parentId,
			Instant from, Instant to, Order order) throws CradleStorageException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		checkTimeBoundaries(fromDateTime, toDateTime, from, to);

		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();
		
		String queryInfo = format("getting child test events of %s from range %s..%s in %s order", parentId, from, to, order);
		CompletableFuture<MappedAsyncPagingIterable<TestEventChildEntity>> future =
				new AsyncOperator<MappedAsyncPagingIterable<TestEventChildEntity>>(semaphore)
						.getFuture(() -> selectExecutor.executeMultiRowResultQuery(() -> order == Order.DIRECT
								? ops.getTestEventChildrenOperator().getTestEventsDirect(instanceUuid, parentId.toString(),
									fromDateTime.toLocalDate(), fromTime, toTime, readAttrs)
								: ops.getTestEventChildrenOperator().getTestEventsReverse(instanceUuid, parentId.toString(),
									fromDateTime.toLocalDate(), fromTime, toTime, readAttrs),
								ops.getTestEventChildConverter(), queryInfo));
		return future.thenApply(result -> new TestEventChildrenMetadataIteratorAdapter(result, pagingSupplies, ops.getTestEventChildConverter(), queryInfo));
	}


	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(Instant from, Instant to, Order order) throws CradleStorageException, IOException
	{
		try
		{
			return doGetTestEventsAsync(from, to, order).get();
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
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsAsync(Instant from, Instant to, Order order)
			throws CradleStorageException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		checkTimeBoundaries(fromDateTime, toDateTime, from, to);

		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();
		
		String queryInfo = format("getting child test events from range %s..%s in %s order", from, to, order);
		CompletableFuture<MappedAsyncPagingIterable<TimeTestEventEntity>> future =
				new AsyncOperator<MappedAsyncPagingIterable<TimeTestEventEntity>>(semaphore)
						.getFuture(() -> selectExecutor.executeMultiRowResultQuery(() -> order == Order.DIRECT
								? ops.getTimeTestEventOperator().getTestEventsDirect(instanceUuid,
										fromDateTime.toLocalDate(), fromTime, toTime, readAttrs)
								: ops.getTimeTestEventOperator().getTestEventsReverse(instanceUuid,
										fromDateTime.toLocalDate(), fromTime, toTime, readAttrs),
								ops.getTimeTestEventConverter(), queryInfo));
		return future.thenApply(result -> new TimeTestEventsMetadataIteratorAdapter(result, pagingSupplies, ops.getTimeTestEventConverter(), queryInfo));
	}


	@Override
	protected Collection<String> doGetStreams() throws IOException
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
	protected Collection<Instant> doGetRootTestEventsDates() throws IOException
	{
		Collection<Instant> result = new ArrayList<>();
		RootTestEventOperator rteOperator = ops.getRootTestEventOperator();
		RootTestEventsDatesConverter converter = ops.getRootTestEventsDatesConverter();
		String queryInfo = "getting root event dates";
		CompletableFuture<MappedAsyncPagingIterable<RootTestEventDateEntity>> future =
				new AsyncOperator<MappedAsyncPagingIterable<RootTestEventDateEntity>>(semaphore).getFuture(
						() -> selectExecutor.executeMultiRowResultQuery(
								() -> rteOperator.getDates(readAttrs), converter, queryInfo));
		try
		{
			PagedIterator<RootTestEventDateEntity> entities =
					future.thenApply(it -> new PagedIterator<>(it, pagingSupplies, converter, queryInfo)).get();
			for (; entities.hasNext(); )
			{
				RootTestEventDateEntity entity = entities.next();
				if (instanceUuid.equals(entity.getInstanceId()))
					result.add(entity.getStartDate().atStartOfDay(TIMEZONE_OFFSET).toInstant());
			}
			return result;
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new IOException("Error occured while " + queryInfo, e);
		}
	}

	@Override
	protected Collection<Instant> doGetTestEventsDates(StoredTestEventId parentId) throws IOException
	{
		Collection<Instant> result = new ArrayList<>();
		TestEventChildrenDatesOperator tecdOperator = ops.getTestEventChildrenDatesOperator();
		TestEventChildDatesConverter converter = ops.getTestEventChildDatesConverter();
		String queryInfo = "getting child event dates of parent " + parentId;
		CompletableFuture<MappedAsyncPagingIterable<TestEventChildDateEntity>> future = 
				new AsyncOperator<MappedAsyncPagingIterable<TestEventChildDateEntity>>(semaphore).getFuture(
						() -> selectExecutor.executeMultiRowResultQuery(
								() -> tecdOperator.get(instanceUuid, parentId.toString(), readAttrs), converter, queryInfo));
		try
		{
			PagedIterator<TestEventChildDateEntity> entities =
					future.thenApply(it -> new PagedIterator<>(it, pagingSupplies, converter, queryInfo)).get();
			for (; entities.hasNext(); )
			{
				TestEventChildDateEntity entity = entities.next();
				result.add(entity.getStartDate().atStartOfDay(TIMEZONE_OFFSET).toInstant());
			}
			return result;
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new IOException("Error occured while " + queryInfo, e);
		}
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
				.getFuture(() -> {
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
					MessageBatchOperator op = rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
					return op.writeMessageBatch(entity, writeAttrs);
				});
		return future.thenAccept(e -> {});
	}

	private CompletableFuture<DetailedMessageBatchEntity> readMessageBatchEntity(StoredMessageId messageId, boolean rawMessage)
	{
		MessageBatchOperator op = rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
		return new AsyncOperator<DetailedMessageBatchEntity>(semaphore)
				.getFuture(() -> selectExecutor.executeSingleRowResultQuery(
						() -> CassandraMessageUtils.getMessageBatch(messageId, op, instanceUuid, readAttrs),
						ops.getMessageBatchConverter(), "getting message batch by id "+messageId));
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

	private void checkTimeBoundaries(LocalDateTime fromDateTime, LocalDateTime toDateTime, Instant originalFrom, Instant originalTo)
			throws CradleStorageException
	{
		LocalDate fromDate = fromDateTime.toLocalDate(),
				toDate = toDateTime.toLocalDate();
		if (!fromDate.equals(toDate))
			throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+originalFrom+"' and '"+originalTo+"'");
	}

	private long getFirstIndex(MessageBatchOperator op, String streamName, Direction direction) throws IOException
	{
		String queryInfo = format("getting first message for stream '%s' and direction '%s'", streamName, direction);
		CompletableFuture<Row> future = new AsyncOperator<Row>(semaphore).getFuture(() ->
						selectExecutor.executeSingleRowResultQuery(
								() -> op.getFirstIndex(instanceUuid, streamName, direction.getLabel(), readAttrs),
								r -> r, queryInfo));
		try
		{
			Row row = future.get();
			return row == null ? EMPTY_MESSAGE_INDEX : row.getLong(MESSAGE_INDEX);
		}
		catch (Exception e)
		{
			throw new IOException("Error occurred while " + queryInfo, e);
		}
	}

	private long getLastIndex(MessageBatchOperator op, String streamName, Direction direction) throws IOException
	{
		String queryInfo = format("getting last message for stream '%s' and direction '%s'", streamName, direction);
		CompletableFuture<Row> future = new AsyncOperator<Row>(semaphore).getFuture(() ->
						selectExecutor.executeSingleRowResultQuery(
								() -> op.getLastIndex(instanceUuid, streamName, direction.getLabel(), readAttrs),
								r -> r, queryInfo));
		try
		{
			Row row = future.get();
			return row == null ? EMPTY_MESSAGE_INDEX : row.getLong(LAST_MESSAGE_INDEX);
		}
		catch (Exception e)
		{
			throw new IOException("Error occurred while " + queryInfo, e);
		}
	}
	
	protected CompletableFuture<DetailedTestEventEntity> storeEvent(StoredTestEvent event)
	{
		return new AsyncOperator<DetailedTestEventEntity>(semaphore).getFuture(() -> {
			DetailedTestEventEntity entity;
			try
			{
				entity = new DetailedTestEventEntity(event, instanceUuid);
			}
			catch (IOException e)
			{
				CompletableFuture<DetailedTestEventEntity> error = new CompletableFuture<>();
				error.completeExceptionally(e);
				return error;
			}

			logger.trace("Executing test event storing query");
			return ops.getTestEventOperator().write(entity, writeAttrs);
		});
	}

	protected CompletableFuture<TimeTestEventEntity> storeTimeEvent(StoredTestEvent event)
	{
		return new AsyncOperator<TimeTestEventEntity>(semaphore).getFuture(() -> {
			TimeTestEventEntity timeEntity;
			try
			{
				timeEntity = new TimeTestEventEntity(event, instanceUuid);
			}
			catch (IOException e)
			{
				CompletableFuture<TimeTestEventEntity> error = new CompletableFuture<>();
				error.completeExceptionally(e);
				return error;
			}

			logger.trace("Executing time/event storing query");
			return ops.getTimeTestEventOperator().writeTestEvent(timeEntity, writeAttrs);
		});
	}

	protected CompletableFuture<RootTestEventEntity> storeRootEvent(StoredTestEvent event)
	{
		return new AsyncOperator<RootTestEventEntity>(semaphore).getFuture(() -> {
			RootTestEventEntity entity = new RootTestEventEntity(event, instanceUuid);

			logger.trace("Executing root event storing query");
			return ops.getRootTestEventOperator().writeTestEvent(entity, writeAttrs);
		});
	}

	protected CompletableFuture<TestEventChildEntity> storeEventInParent(StoredTestEvent event)
	{
		return new AsyncOperator<TestEventChildEntity>(semaphore).getFuture(() -> {
			TestEventChildEntity entity;
			try
			{
				entity = new TestEventChildEntity(event, instanceUuid);
			}
			catch (IOException e)
			{
				CompletableFuture<TestEventChildEntity> error = new CompletableFuture<>();
				error.completeExceptionally(e);
				return error;
			}

			logger.trace("Executing parent/event storing query");
			return ops.getTestEventChildrenOperator().writeTestEvent(entity, writeAttrs);
		});
	}

	protected CompletableFuture<TestEventChildDateEntity> storeEventDateInParent(StoredTestEvent event)
	{
		return new AsyncOperator<TestEventChildDateEntity>(semaphore).getFuture(() -> {
			TestEventChildDateEntity entity = new TestEventChildDateEntity(event, instanceUuid);

			logger.trace("Executing parent/event date storing query");
			return ops.getTestEventChildrenDatesOperator().writeTestEventDate(entity, writeAttrs);
		});
	}


	protected CompletableFuture<Void> storeMessagesOfTestEvent(String eventId, List<String> messageIds)
	{
		List<CompletableFuture<TestEventMessagesEntity>> futures = new ArrayList<>();
		TestEventMessagesOperator op = ops.getTestEventMessagesOperator();
		int msgsSize = messageIds.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + TEST_EVENTS_MSGS_LINK_MAX_MSGS, msgsSize);
			Set<String> curMsgsIds = new HashSet<>(messageIds.subList(left, right));
			logger.trace("Linking {} message(s) to test event {}", curMsgsIds.size(), eventId);

			TestEventMessagesEntity entity = new TestEventMessagesEntity();
			entity.setInstanceId(getInstanceUuid());
			entity.setEventId(eventId);
			entity.setMessageIds(curMsgsIds);

			futures.add(new AsyncOperator<TestEventMessagesEntity>(semaphore)
					.getFuture(() -> op.writeMessages(entity, writeAttrs)));

			left = right - 1;
		}
		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
	}

	protected CompletableFuture<Void> storeTestEventOfMessages(List<String> messageIds, String eventId, StoredTestEventId batchId)
	{
		String batchIdString = batchId != null ? batchId.toString() : null;
		List<CompletableFuture<MessageTestEventEntity>> futures = new ArrayList<>();
		MessageTestEventOperator op = ops.getMessageTestEventOperator();
		for (String id : messageIds)
		{
			logger.trace("Linking test event {} to message {}", eventId, id);

			MessageTestEventEntity entity = new MessageTestEventEntity();
			entity.setInstanceId(getInstanceUuid());
			entity.setMessageId(id);
			entity.setEventId(eventId);
			if (batchIdString != null)
				entity.setBatchId(batchIdString);

			futures.add(new AsyncOperator<MessageTestEventEntity>(semaphore)
					.getFuture(() -> op.writeTestEvent(entity, writeAttrs)));
		}
		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
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
		String id = event.getId().toString(),
				parentId = event.getParentId() != null ? event.getParentId().toString() : null;
		LocalDateTime ldt = LocalDateTime.ofInstant(event.getStartTimestamp(), TIMEZONE_OFFSET);
		LocalDate ld = ldt.toLocalDate();
		LocalTime lt = ldt.toLocalTime();

		CompletableFuture<AsyncResultSet> result1 = new AsyncOperator<AsyncResultSet>(semaphore)
				.getFuture(() -> ops.getTestEventOperator().updateStatus(instanceUuid, id, success, writeAttrs)),
				result2 = new AsyncOperator<AsyncResultSet>(semaphore)
						.getFuture(() -> ops.getTimeTestEventOperator().updateStatus(instanceUuid, ld, lt, id, success, writeAttrs));
		CompletableFuture<AsyncResultSet> result3;
		if (parentId != null)
			result3 = new AsyncOperator<AsyncResultSet>(semaphore)
					.getFuture(() -> ops.getTestEventChildrenOperator().updateStatus(instanceUuid, parentId, ld, lt, id, success, writeAttrs));
		else
			result3 = new AsyncOperator<AsyncResultSet>(semaphore)
					.getFuture(() -> ops.getRootTestEventOperator().updateStatus(instanceUuid, ld, lt, id, success, writeAttrs));
		return CompletableFuture.allOf(result1, result2, result3);
	}

	protected CompletableFuture<Void> failEventAndParents(StoredTestEventId eventId)
	{
		return getTestEventAsync(eventId)
				.thenComposeAsync((event) -> {
					if (event == null || !event.isSuccess())  //Invalid event ID or event is already failed, which means that its parents are already updated
						return CompletableFuture.completedFuture(null);

					CompletableFuture<Void> update = doUpdateEventStatusAsync(event, false);
					if (event.getParentId() != null)
						return update.thenComposeAsync((u) -> failEventAndParents(event.getParentId()));
					return update;
				});
	}
}
