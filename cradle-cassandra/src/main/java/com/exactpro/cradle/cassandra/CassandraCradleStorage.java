/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.TimeRelation;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.intervals.CassandraIntervalsWorker;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.cassandra.dao.messages.StreamEntity;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageEntity;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.dao.testevents.DetailedTestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventDateEntity;
import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildDateEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventEntity;
import com.exactpro.cradle.cassandra.iterators.*;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.retry.AsyncExecutor;
import com.exactpro.cradle.cassandra.retry.SyncExecutor;
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
import com.exactpro.cradle.utils.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.*;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
	public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;

	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	private final CassandraSemaphore semaphore;
	private final CradleObjectsFactory objectsFactory;
	private final int syncRetries,
			asyncRetries,
			retryDelay;
	private final ExecutorService composingService;
	private final boolean ownedComposingService;

	private CassandraOperators ops;

	private UUID instanceUuid;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	private int resultPageSize;

	private QueryExecutor exec;
	private SyncExecutor syncExecutor;
	private AsyncExecutor asyncExecutor;
	
	private TestEventsMessagesLinker testEventsMessagesLinker;
	private IntervalsWorker intervalsWorker;

	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		CassandraConnectionSettings conSettings = connection.getSettings();
		
		this.connection = connection;
		this.settings = settings;
		semaphore = new CassandraSemaphore(conSettings.getMaxParallelQueries());
		objectsFactory = new CradleObjectsFactory(settings.getMaxMessageBatchSize(), settings.getMaxTestEventBatchSize());
		resultPageSize = conSettings.getResultPageSize();
		syncRetries = conSettings.getMaxSyncRetries();
		asyncRetries = conSettings.getMaxAsyncRetries();
		retryDelay = conSettings.getRetryDelay();
		
		if (conSettings.getComposingService() == null)
		{
			ownedComposingService = true;
			composingService = Executors.newFixedThreadPool(5);
		}
		else
		{
			ownedComposingService = false;
			composingService = conSettings.getComposingService();
		}
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
			exec = new QueryExecutor(connection.getSession(),
					settings.getTimeout(), settings.getWriteConsistencyLevel(), settings.getReadConsistencyLevel());
			syncExecutor = new SyncExecutor(retryDelay);
			asyncExecutor = new AsyncExecutor(Executors.newSingleThreadExecutor(), composingService, retryDelay);

			if (prepareStorage)
			{
				logger.info("Creating/updating schema...");
				createTables();
				logger.info("All needed tables created");
			}
			else
				logger.info("Schema creation/update skipped");

			instanceUuid = getInstanceId(instanceName);
			CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(connection.getSession()).build();
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

			testEventsMessagesLinker = new CassandraTestEventsMessagesLinker(ops.getTestEventMessagesOperator(), ops.getMessageTestEventOperator(),
					instanceUuid, readAttrs, semaphore);

			intervalsWorker = new CassandraIntervalsWorker(semaphore, instanceUuid, writeAttrs, readAttrs, ops.getIntervalOperator());

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
		if (asyncExecutor != null)
		{
			logger.info("Shutting down asynchronous request executor...");
			asyncExecutor.dispose();
		}
		
		if (ownedComposingService)
		{
			logger.info("Shutting down composing service...");
			composingService.shutdownNow();
		}
		
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
		DetailedMessageBatchEntity entity = new DetailedMessageBatchEntity(batch, instanceUuid);
		try
		{
			logger.trace("Executing query to store message batch {}", batch.getId());
			syncExecutor.submit("store message batch "+batch.getId(), syncRetries,
					() -> writeMessageBatchSimple(entity, batch, true));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch)
	{
		return writeMessageBatch(batch, true);
	}

	@Override
	protected void doStoreProcessedMessageBatch(StoredMessageBatch batch) throws IOException
	{
		DetailedMessageBatchEntity entity = new DetailedMessageBatchEntity(batch, instanceUuid);
		try
		{
			logger.trace("Executing query to store processed message batch {}", batch.getId());
			syncExecutor.submit("store processed message batch "+batch.getId(), syncRetries,
					() -> writeMessageBatchSimple(entity, batch, false));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing processed message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreProcessedMessageBatchAsync(StoredMessageBatch batch)
	{
		return writeMessageBatch(batch, false);
	}


	@Override
	protected void doStoreTestEvent(StoredTestEvent event) throws IOException
	{
		DetailedTestEventEntity entity = new DetailedTestEventEntity(event, instanceUuid);
		try
		{
			logger.trace("Executing query to store test event {}", event.getId());
			syncExecutor.submit("store test event "+event.getId(), syncRetries,
					() -> writeTestEventSimple(entity, event));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing test event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(StoredTestEvent event)
	{
		return writeTestEvent(event);
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
		return entityFuture.thenCompose((entity) -> {
			if (entity == null)
				return CompletableFuture.completedFuture(null);
			Collection<StoredMessage> msgs;
			try
			{
				msgs = MessageUtils.bytesToMessages(entity.getContent(), entity.isCompressed());
			}
			catch (IOException e)
			{
				throw new CompletionException("Error while reading message batch", e);
			}
			return CompletableFuture.completedFuture(msgs);
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
	protected long doGetLastMessageIndex(String streamName, Direction direction) throws IOException
	{
		return getLastIndex(ops.getMessageBatchOperator(), streamName, direction);
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
		CompletableFuture<TimeMessageEntity> result = timeRelation == TimeRelation.BEFORE
				? ops.getTimeMessageOperator()
						.getNearestMessageBefore(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs)
				: ops.getTimeMessageOperator()
						.getNearestMessageAfter(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs);

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
		CompletableFuture<MappedAsyncPagingIterable<TestEventEntity>> future =
				new AsyncOperator<MappedAsyncPagingIterable<TestEventEntity>>(semaphore)
						.getFuture(() -> ops.getTestEventOperator().getComplete(instanceUuid,
								id.stream().map(StoredTestEventId::toString).collect(toList()), readAttrs));
		
		return future.thenApply(TestEventDataIteratorAdapter::new);
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
		return doGetDetailedMessageBatchEntities(filter).thenApply(it -> new MessagesIteratorAdapter(filter, it));
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
		return doGetDetailedMessageBatchEntities(filter).thenApply(it -> new StoredMessageBatchAdapter(it, objectsFactory, filter == null ? 0 : filter.getLimit()));
	}

	private CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> doGetDetailedMessageBatchEntities(StoredMessageFilter filter)
	{
		MessageBatchOperator op = ops.getMessageBatchOperator();
		return new AsyncOperator<MappedAsyncPagingIterable<DetailedMessageBatchEntity>>(semaphore)
				.getFuture(() -> op.filterMessages(instanceUuid, filter, semaphore, op, readAttrs));
	}

	@Override
	protected Iterable<StoredTestEventMetadata> doGetRootTestEvents(Instant from, Instant to) throws CradleStorageException, IOException
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
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetRootTestEventsAsync(Instant from, Instant to) throws CradleStorageException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		checkTimeBoundaries(fromDateTime, toDateTime, from, to);

		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();

		CompletableFuture<MappedAsyncPagingIterable<RootTestEventEntity>> future = new AsyncOperator<MappedAsyncPagingIterable<RootTestEventEntity>>(semaphore)
				.getFuture(() -> ops.getRootTestEventOperator().getTestEvents(instanceUuid, fromDateTime.toLocalDate(), fromTime, toTime, readAttrs));
		return future.thenApply(it -> new RootTestEventsMetadataIteratorAdapter(it));
	}


	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(StoredTestEventId parentId, Instant from, Instant to)
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
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsAsync(StoredTestEventId parentId, Instant from, Instant to) throws CradleStorageException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		checkTimeBoundaries(fromDateTime, toDateTime, from, to);

		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();

		CompletableFuture<MappedAsyncPagingIterable<TestEventChildEntity>> future = new AsyncOperator<MappedAsyncPagingIterable<TestEventChildEntity>>(semaphore)
				.getFuture(() -> ops.getTestEventChildrenOperator().getTestEvents(instanceUuid, parentId.toString(),
						fromDateTime.toLocalDate(), fromTime, toTime, readAttrs));
		return future.thenApply(it -> new TestEventChildrenMetadataIteratorAdapter(it));
	}


	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(Instant from, Instant to) throws CradleStorageException, IOException
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
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsAsync(Instant from, Instant to)
			throws CradleStorageException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		checkTimeBoundaries(fromDateTime, toDateTime, from, to);

		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();

		CompletableFuture<MappedAsyncPagingIterable<TimeTestEventEntity>> future = new AsyncOperator<MappedAsyncPagingIterable<TimeTestEventEntity>>(semaphore)
				.getFuture(() -> ops.getTimeTestEventOperator().getTestEvents(instanceUuid,
						fromDateTime.toLocalDate(), fromTime, toTime, readAttrs));
		return future.thenApply(it -> new TimeTestEventsMetadataIteratorAdapter(it));
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
		List<Instant> result = new ArrayList<>();
		for (RootTestEventDateEntity entity : ops.getRootTestEventOperator().getDates(readAttrs))
		{
			if (instanceUuid.equals(entity.getInstanceId()))
				result.add(entity.getStartDate().atStartOfDay(TIMEZONE_OFFSET).toInstant());
		}
		result.sort(null);
		return result;
	}

	@Override
	protected Collection<Instant> doGetTestEventsDates(StoredTestEventId parentId) throws IOException
	{
		Collection<Instant> result = new ArrayList<>();
		for (TestEventChildDateEntity entity : ops.getTestEventChildrenDatesOperator().get(instanceUuid, parentId.toString(), readAttrs))
			result.add(entity.getStartDate().atStartOfDay(TIMEZONE_OFFSET).toInstant());
		return result;
	}

	@Override
	public CradleObjectsFactory getObjectsFactory()
	{
		return objectsFactory;
	}
	
	@Override
	public int getActiveAsyncRequests()
	{
		return asyncExecutor.getActiveRequests();
	}
	
	@Override
	public int getPendingAsyncRequests()
	{
		return asyncExecutor.getPendingRequests();
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

	
	private CompletableFuture<?> writeMessageBatchSimple(DetailedMessageBatchEntity entity, StoredMessageBatch batch, boolean rawMessage)
	{
		if (rawMessage)
			return ops.getMessageBatchOperator().writeMessageBatch(entity, writeAttrs)
					.thenComposeAsync(r -> writeTimeMessages(batch.getMessages()), composingService);
		return ops.getProcessedMessageBatchOperator().writeMessageBatch(entity, writeAttrs);
	}
	
	private CompletableFuture<Void> writeMessageBatch(StoredMessageBatch batch, boolean rawMessage)
	{
		DetailedMessageBatchEntity entity;
		try
		{
			entity = new DetailedMessageBatchEntity(batch, instanceUuid);
		}
		catch (IOException e)
		{
			CompletableFuture<Void> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			return error;
		}
		
		return asyncExecutor.submit(rawMessage ? "store message batch "+batch.getId() : "store processed message batch "+batch.getId(), asyncRetries,
				() -> {
					return writeMessageBatchSimple(entity, batch, rawMessage)
							.thenAccept(r -> {});
		});
	}
	
	private CompletableFuture<TimeMessageEntity> writeTimeMessages(Collection<StoredMessage> messages)
	{
		CompletableFuture<TimeMessageEntity> result = CompletableFuture.completedFuture(null);
		Instant ts = null;
		TimeMessageOperator op = ops.getTimeMessageOperator();
		for (StoredMessage msg : messages)
		{
			Instant msgSeconds = TimeUtils.cutNanos(msg.getTimestamp());
			if (msgSeconds.equals(ts))
				continue;
			
			ts = msgSeconds;
			
			TimeMessageEntity timeEntity = new TimeMessageEntity(msg, instanceUuid);
			result = result.thenComposeAsync(r -> {
				logger.trace("Executing time/message storing query for message {}", msg.getId());
				return op.writeMessage(timeEntity, writeAttrs);
			}, composingService);
		}
		return result;
	}

	private CompletableFuture<DetailedMessageBatchEntity> readMessageBatchEntity(StoredMessageId messageId, boolean rawMessage)
	{
		MessageBatchOperator op = rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
		return CassandraMessageUtils.getMessageBatch(messageId, op, semaphore, instanceUuid, readAttrs);
	}

	private CompletableFuture<StoredMessage> readMessage(StoredMessageId id, boolean rawMessage)
	{
		CompletableFuture<DetailedMessageBatchEntity> entityFuture = readMessageBatchEntity(id, rawMessage);
		return entityFuture.thenCompose((entity) -> {
			if (entity == null)
				return CompletableFuture.completedFuture(null);
			StoredMessage msg;
			try
			{
				msg = MessageUtils.bytesToOneMessage(entity.getContent(), entity.isCompressed(), id);
			}
			catch (IOException e)
			{
				throw new CompletionException("Error while reading message", e);
			}
			return CompletableFuture.completedFuture(msg);
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

	private long getLastIndex(MessageBatchOperator op, String streamName, Direction direction)
	{
		DetailedMessageBatchEntity result = op.getLastIndex(instanceUuid, streamName, direction.getLabel(), readAttrs);
		return result != null ? result.getLastMessageIndex() : -1;
	}
	
	
	private CompletableFuture<?> writeTestEventSimple(DetailedTestEventEntity entity, StoredTestEvent event)
	{
		return ops.getTestEventOperator().write(entity, writeAttrs)
				.thenComposeAsync(r -> storeTimeEvent(event), composingService)
				.thenComposeAsync(r -> {
					if (event.getParentId() != null)
					{
						return storeEventInParent(event)
								.thenComposeAsync(r2 -> storeEventDateInParent(event), composingService)
								.thenComposeAsync(r2 -> updateParentTestEvents(event), composingService)
								.thenAccept(r2 -> {});
					}
					else
						return storeRootEvent(event).thenAccept(r2 -> {});
				}, composingService);
	}

	protected CompletableFuture<Void> writeTestEvent(StoredTestEvent event)
	{
		DetailedTestEventEntity entity;
		try
		{
			entity = new DetailedTestEventEntity(event, instanceUuid);
		}
		catch (IOException e)
		{
			CompletableFuture<Void> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			return error;
		}
		
		return asyncExecutor.submit("store test event "+event.getId(), asyncRetries,
				() -> {
					return writeTestEventSimple(entity, event)
							.thenAccept(r -> {});
		});
	}

	private CompletableFuture<TimeTestEventEntity> storeTimeEvent(StoredTestEvent event)
	{
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
		
		logger.trace("Executing time/event storing query for {}", event.getId());
		return ops.getTimeTestEventOperator().writeTestEvent(timeEntity, writeAttrs);
	}

	protected CompletableFuture<RootTestEventEntity> storeRootEvent(StoredTestEvent event)
	{
		RootTestEventEntity entity = new RootTestEventEntity(event, instanceUuid);
		
		logger.trace("Executing root event storing query for {}", event.getId());
		return ops.getRootTestEventOperator().writeTestEvent(entity, writeAttrs);
	}

	protected CompletableFuture<TestEventChildEntity> storeEventInParent(StoredTestEvent event)
	{
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
		
		logger.trace("Executing parent/event storing query for {}", event.getId());
		return ops.getTestEventChildrenOperator().writeTestEvent(entity, writeAttrs);
	}

	protected CompletableFuture<TestEventChildDateEntity> storeEventDateInParent(StoredTestEvent event)
	{
		TestEventChildDateEntity entity = new TestEventChildDateEntity(event, instanceUuid);
		
		logger.trace("Executing parent/event date storing query for {}", event.getId());
		return ops.getTestEventChildrenDatesOperator().writeTestEventDate(entity, writeAttrs);
	}
	
	protected CompletableFuture<Void> updateParentTestEvents(StoredTestEvent event)
	{
		if (event.isSuccess())
			return CompletableFuture.completedFuture(null);
		
		logger.trace("Updating parent of {}", event.getId());
		return failEventAndParents(event.getParentId());
	}
	
	protected CompletableFuture<Void> failEventAndParents(StoredTestEventId eventId)
	{
		return getTestEventAsync(eventId)
				.thenComposeAsync((event) -> {
					if (event == null || !event.isSuccess())  //Invalid event ID or event is already failed, which means that its parents are already updated
						return CompletableFuture.completedFuture(null);
					
					CompletableFuture<Void> update = doUpdateEventStatusAsync(event, false);
					if (event.getParentId() != null)
						return update.thenComposeAsync((u) -> failEventAndParents(event.getParentId()), composingService);
					return update;
				}, composingService);
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

		return ops.getTestEventOperator().updateStatus(instanceUuid, id, success, writeAttrs)
				.thenComposeAsync(r -> ops.getTimeTestEventOperator().updateStatus(instanceUuid, ld, lt, id, success, writeAttrs), composingService)
				.thenComposeAsync(r -> {
					if (parentId != null)
						return ops.getTestEventChildrenOperator().updateStatus(instanceUuid, parentId, ld, lt, id, success, writeAttrs);
					return ops.getRootTestEventOperator().updateStatus(instanceUuid, ld, lt, id, success, writeAttrs);
				}, composingService)
				.thenAccept(r -> {});
	}
}
