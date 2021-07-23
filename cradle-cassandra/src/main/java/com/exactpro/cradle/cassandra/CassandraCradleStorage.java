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
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.intervals.CassandraIntervalsWorker;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.StreamEntity;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageEntity;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.retry.AsyncExecutor;
import com.exactpro.cradle.cassandra.retry.SyncExecutor;
import com.exactpro.cradle.cassandra.utils.DateTimeUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.TimestampRange;
import com.exactpro.cradle.exceptions.CradleStorageException;
import com.exactpro.cradle.exceptions.TooManyRequestsException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.*;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
	
	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	private final CradleObjectsFactory objectsFactory;
	private final ExecutorService composingService;
	private final boolean ownedComposingService;

	private CassandraOperators ops;

	private UUID instanceUuid;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	
	private QueryExecutor exec;
	private SyncExecutor syncExecutor;
	private AsyncExecutor asyncExecutor;
	
	private TestEventsMessagesLinker testEventsMessagesLinker;
	private MessagesWorker messagesWorker;
	private TestEventsWorker testEventsWorker;
	private IntervalsWorker intervalsWorker;

	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		CassandraConnectionSettings conSettings = connection.getSettings();
		
		this.connection = connection;
		this.settings = settings;
		objectsFactory = new CradleObjectsFactory(settings.getMaxMessageBatchSize(), settings.getMaxTestEventBatchSize());
		
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
			CassandraConnectionSettings conSettings = connection.getSettings();
			
			exec = new QueryExecutor(connection.getSession(),
					settings.getTimeout(), settings.getWriteConsistencyLevel(), settings.getReadConsistencyLevel());
			
			int retryDelay = conSettings.getRetryDelay();
			syncExecutor = new SyncExecutor(conSettings.getMaxSyncRetries(), retryDelay);
			asyncExecutor = new AsyncExecutor(conSettings.getMaxParallelQueries(), composingService, conSettings.getMaxAsyncRetries(), retryDelay);
			
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
			int resultPageSize = conSettings.getResultPageSize();
			writeAttrs = builder -> builder.setConsistencyLevel(settings.getWriteConsistencyLevel())
					.setTimeout(timeout);
			readAttrs = builder -> builder.setConsistencyLevel(settings.getReadConsistencyLevel())
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			strictReadAttrs = builder -> builder.setConsistencyLevel(ConsistencyLevel.ALL)
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			
			testEventsMessagesLinker = new CassandraTestEventsMessagesLinker(ops.getTestEventMessagesOperator(), ops.getMessageTestEventOperator(),
					instanceUuid, syncExecutor, asyncExecutor, composingService, readAttrs);
			messagesWorker = new MessagesWorker(instanceUuid, ops, objectsFactory, writeAttrs, readAttrs, composingService);
			testEventsWorker = new TestEventsWorker(instanceUuid, ops, writeAttrs, readAttrs, composingService);
			intervalsWorker = new CassandraIntervalsWorker(instanceUuid, writeAttrs, readAttrs, ops.getIntervalOperator(), syncExecutor, asyncExecutor);
			
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
			syncExecutor.submit("store message batch "+batch.getId(), 
					() -> messagesWorker.writeMessageBatch(entity, batch, true));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch) throws IOException, TooManyRequestsException
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
			syncExecutor.submit("store processed message batch "+batch.getId(), 
					() -> messagesWorker.writeMessageBatch(entity, batch, false));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing processed message batch "+batch.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreProcessedMessageBatchAsync(StoredMessageBatch batch) throws IOException, TooManyRequestsException
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
			syncExecutor.submit("store test event "+event.getId(), 
					() -> testEventsWorker.writeEvent(entity, event));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing test event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(StoredTestEvent event) throws IOException, TooManyRequestsException
	{
		DetailedTestEventEntity entity = new DetailedTestEventEntity(event, instanceUuid);
		return asyncExecutor.submit("store test event "+event.getId(), 
				() -> testEventsWorker.writeEvent(entity, event).thenAccept(r -> {}));
	}

	@Override
	protected void doStoreTestEventMessagesLink(StoredTestEventId eventId, StoredTestEventId batchId, 
			Collection<StoredMessageId> messageIds) throws IOException
	{
		try
		{
			syncExecutor.submit("store link between "+eventId+" and "+messageIds.size()+" message(s)", 
					() -> writeTestEventMessagesLinks(eventId, batchId, messageIds));
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing link between test event "+eventId+" and "+messageIds.size()+" message(s)", e);
		}
	}

	@Override
	protected CompletableFuture<Void> doStoreTestEventMessagesLinkAsync(StoredTestEventId eventId, StoredTestEventId batchId, 
			Collection<StoredMessageId> messageIds) throws IOException, TooManyRequestsException
	{
		return asyncExecutor.submit("store link between "+eventId+" and "+messageIds.size()+" messsage(s)", 
				() -> writeTestEventMessagesLinks(eventId, batchId, messageIds).thenAccept(r -> {}));
	}

	@Override
	protected StoredMessage doGetMessage(StoredMessageId id) throws IOException
	{
		try
		{
			return syncExecutor.submit("get message "+id, 
					() -> messagesWorker.readMessage(id, true));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message "+id, e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get message "+id, 
				() -> messagesWorker.readMessage(id, true));
	}

	@Override
	protected Collection<StoredMessage> doGetMessageBatch(StoredMessageId id) throws IOException
	{
		try
		{
			return syncExecutor.submit("get message batch "+id, 
					() -> messagesWorker.readMessageBatch(id));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message batch "+id, e);
		}
	}

	@Override
	protected CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get message batch "+id, 
				() -> messagesWorker.readMessageBatch(id));
	}

	@Override
	protected StoredMessage doGetProcessedMessage(StoredMessageId id) throws IOException
	{
		try
		{
			return syncExecutor.submit("get processed message "+id, 
					() -> messagesWorker.readMessage(id, false));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting processed message "+id, e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessage> doGetProcessedMessageAsync(StoredMessageId id) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get processed message "+id, 
				() -> messagesWorker.readMessage(id, false));
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
			return syncExecutor.submit("get message ID with timestamp "+timeRelation.getLabel()+" "+timestamp+", stream "+streamName+", "+direction.getLabel(), 
					() -> readNearestMessageId(streamName, direction, timestamp, timeRelation));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting nearest message ID", e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessageId> doGetNearestMessageIdAsync(String streamName, Direction direction, Instant timestamp, 
			TimeRelation timeRelation) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get message ID with timestamp "+timeRelation.getLabel()+" "+timestamp+", stream "+streamName+", "+direction.getLabel(), 
				() -> readNearestMessageId(streamName, direction, timestamp, timeRelation));
	}

	@Override
	protected StoredTestEventWrapper doGetTestEvent(StoredTestEventId id) throws IOException
	{
		try
		{
			return syncExecutor.submit("get event "+id, 
					() -> testEventsWorker.readEvent(id));
		}
		catch (Exception e)
		{
			throw new IOException("Could not get test event", e);
		}
	}

	@Override
	protected CompletableFuture<StoredTestEventWrapper> doGetTestEventAsync(StoredTestEventId id) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get event "+id, 
				() -> testEventsWorker.readEvent(id));
	}

	@Override
	protected Iterable<StoredTestEventWrapper> doGetCompleteTestEvents(Set<StoredTestEventId> ids) throws IOException
	{
		try
		{
			return syncExecutor.submit("get "+ids.size()+" test event(s)", 
					() -> testEventsWorker.readCompleteEvents(ids));
		}
		catch (Exception e)
		{
			throw new IOException("Could not get test events", e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetCompleteTestEventsAsync(Set<StoredTestEventId> ids) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get "+ids.size()+" test event(s)", 
				() -> testEventsWorker.readCompleteEvents(ids));
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
			return syncExecutor.submit("get messages filtered by "+filter, 
					() -> messagesWorker.readMessages(filter));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting messages filtered by "+filter, e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get messages filtered by "+filter, 
				() -> messagesWorker.readMessages(filter));
	}


	@Override
	protected Iterable<StoredMessageBatch> doGetMessagesBatches(StoredMessageFilter filter) throws IOException
	{
		try
		{
			return syncExecutor.submit("get message batches filtered by "+filter, 
					() -> messagesWorker.readMessageBatches(filter));
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message batches filtered by "+filter, e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter) throws TooManyRequestsException
	{
		return asyncExecutor.submit("get message batches filtered by "+filter, 
				() -> messagesWorker.readMessageBatches(filter));
	}
	
	
	@Override
	protected Iterable<StoredTestEventMetadata> doGetRootTestEvents(Instant from, Instant to, Order order) 
			throws CradleStorageException, IOException
	{
		try
		{
			TimestampRange range = new TimestampRange(from, to);
			return syncExecutor.submit("get root test events from range "+from+".."+to
					+" in "+(order == Order.REVERSE ? "reversed" : "direct")+" order", 
					() -> testEventsWorker.readRootEvents(range, order));
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
			Order order) throws CradleStorageException, TooManyRequestsException
	{
		TimestampRange range = new TimestampRange(from, to);
		return asyncExecutor.submit("get root test events from range "+from+".."+to
				+" in "+(order == Order.REVERSE ? "reversed" : "direct")+" order", 
				() -> testEventsWorker.readRootEvents(range, order));
	}


	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(StoredTestEventId parentId, Instant from, Instant to, Order order) 
			throws CradleStorageException, IOException
	{
		try
		{
			TimestampRange range = new TimestampRange(from, to);
			return syncExecutor.submit("get child test events of "+parentId+" from range "+from+".."+to
					+" in "+(order == Order.REVERSE ? "reversed" : "direct")+" order", 
					() -> testEventsWorker.readEvents(parentId, range, order));
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
			Instant from, Instant to, Order order) throws CradleStorageException, TooManyRequestsException
	{
		TimestampRange range = new TimestampRange(from, to);
		return asyncExecutor.submit("get child test events of "+parentId+" from range "+from+".."+to
				+" in "+(order == Order.REVERSE ? "reversed" : "direct")+" order", 
				() -> testEventsWorker.readEvents(parentId, range, order));
	}


	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(Instant from, Instant to, Order order) throws CradleStorageException, IOException
	{
		try
		{
			TimestampRange range = new TimestampRange(from, to);
			return syncExecutor.submit("get test events from range "+from+".."+to
					+" in "+(order == Order.REVERSE ? "reversed" : "direct")+" order", 
					() -> testEventsWorker.readEvents(range, order));
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
			throws CradleStorageException, TooManyRequestsException
	{
		TimestampRange range = new TimestampRange(from, to);
		return asyncExecutor.submit("get test events from range "+from+".."+to
				+" in "+(order == Order.REVERSE ? "reversed" : "direct")+" order",
				() -> testEventsWorker.readEvents(range, order));
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
				result.add(entity.getStartDate().atStartOfDay(DateTimeUtils.TIMEZONE_OFFSET).toInstant());
		}
		result.sort(null);
		return result;
	}

	@Override
	protected Collection<Instant> doGetTestEventsDates(StoredTestEventId parentId) throws IOException
	{
		Collection<Instant> result = new ArrayList<>();
		for (TestEventChildDateEntity entity : ops.getTestEventChildrenDatesOperator().get(instanceUuid, parentId.toString(), readAttrs))
			result.add(entity.getStartDate().atStartOfDay(DateTimeUtils.TIMEZONE_OFFSET).toInstant());
		return result;
	}
	
	
	@Override
	protected void doUpdateEventStatus(StoredTestEventWrapper event, boolean success) throws IOException
	{
		try
		{
			syncExecutor.submit("update status of event "+event.getId(), 
					() -> testEventsWorker.updateStatus(event, success));
		}
		catch (Exception e)
		{
			throw new IOException("Error while updating status of event "+event.getId(), e);
		}
	}

	@Override
	protected CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEventWrapper event, boolean success) throws TooManyRequestsException
	{
		return asyncExecutor.submit("update status of event "+event.getId(), 
				() -> testEventsWorker.updateStatus(event, success));
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
	
	
	private CompletableFuture<Void> writeMessageBatch(StoredMessageBatch batch, boolean rawMessage) throws IOException, TooManyRequestsException
	{
		DetailedMessageBatchEntity entity = new DetailedMessageBatchEntity(batch, instanceUuid);
		return asyncExecutor.submit(rawMessage ? "store message batch "+batch.getId() : "store processed message batch "+batch.getId(), 
				() -> messagesWorker.writeMessageBatch(entity, batch, rawMessage).thenAccept(r -> {}));
	}
	
	
	private long getLastIndex(MessageBatchOperator op, String streamName, Direction direction)
	{
		DetailedMessageBatchEntity result = op.getLastIndex(instanceUuid, streamName, direction.getLabel(), readAttrs);
		return result != null ? result.getLastMessageIndex() : -1;
	}
	
	private CompletableFuture<StoredMessageId> readNearestMessageId(String streamName, Direction direction, Instant timestamp, TimeRelation timeRelation)
	{
		LocalDateTime messageDateTime = DateTimeUtils.toDateTime(timestamp);
		CompletableFuture<TimeMessageEntity> entityFuture = timeRelation == TimeRelation.BEFORE
				? ops.getTimeMessageOperator()
						.getNearestMessageBefore(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs)
				: ops.getTimeMessageOperator()
						.getNearestMessageAfter(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs);
		
		return entityFuture.thenApply(entity -> entity == null ? null : entity.createMessageId());
	}
	
	
	protected CompletableFuture<TestEventMessagesEntity> writeTestEventMessagesLinks(StoredTestEventId eventId, StoredTestEventId batchId, 
			Collection<StoredMessageId> messageIds)
	{
		List<String> messageIdsStrings = messageIds.stream().map(StoredMessageId::toString).collect(toList());
		String eventIdString = eventId.toString();
		
		CompletableFuture<TestEventMessagesEntity> result = CompletableFuture.completedFuture(null);
		TestEventMessagesOperator op = ops.getTestEventMessagesOperator();
		int msgsSize = messageIds.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + TEST_EVENTS_MSGS_LINK_MAX_MSGS, msgsSize);
			Set<String> curMsgsIds = new HashSet<>(messageIdsStrings.subList(left, right));
			
			TestEventMessagesEntity entity = new TestEventMessagesEntity();
			entity.setInstanceId(getInstanceUuid());
			entity.setEventId(eventIdString);
			entity.setMessageIds(curMsgsIds);
			
			result = result.thenComposeAsync(r -> {
				logger.trace("Linking {} message(s) to test event {}", curMsgsIds.size(), eventId);
				return op.writeMessages(entity, writeAttrs);
			}, composingService);

			left = right - 1;
		}
		return result;
	}
}