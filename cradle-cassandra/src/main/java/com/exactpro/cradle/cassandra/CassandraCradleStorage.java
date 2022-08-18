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
import com.exactpro.cradle.cassandra.dao.testevents.converters.DateEventEntityConverter;
import com.exactpro.cradle.cassandra.iterators.*;
import com.exactpro.cradle.cassandra.retries.*;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.INSTANCES_TABLE_DEFAULT_NAME;
import static com.exactpro.cradle.cassandra.StorageConstants.*;
import static java.lang.String.format;

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
	private PagingSupplies pagingSupplies;

	private IntervalsWorker intervalsWorker;

	private EventBatchDurationWorker eventBatchDurationWorker;

	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		CassandraConnectionSettings conSettings = connection.getSettings();

		this.connection = connection;
		this.settings = settings;
		this.semaphore = new CassandraSemaphore(conSettings.getMaxParallelQueries());
		this.objectsFactory =
				new CradleObjectsFactory(settings.getMaxMessageBatchSize(), settings.getMaxTestEventBatchSize());
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
			selectExecutor = new SelectQueryExecutor(session, semaphore, multiRowResultExecPolicy, singleRowResultExecPolicy);
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

			eventBatchDurationWorker = new EventBatchDurationWorker(
					new EventBatchDurationCache(settings.getEventBatchDurationCacheSize()),
					ops.getEventBatchMaxDurationOperator(),
					readAttrs,
					writeAttrs,
					settings.getEventBatchDurationMillis());

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
	protected void doStoreGroupedMessageBatch(StoredGroupMessageBatch batch, String groupName) throws IOException
	{
		try
		{
			doStoreGroupedMessageBatchAsync(batch, groupName).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while storing message batch with group name"+groupName, e);
		}
	}
	
	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch)
	{
		logger.debug("Creating detailed message batch entity from message batch with id {}", batch.getId());
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
		return new AsyncOperator<Void>(semaphore).getFuture(() -> doWriteMessage(entity, true));
	}
	
	@Override
	protected CompletableFuture<Void> doStoreGroupedMessageBatchAsync(StoredGroupMessageBatch batch, String groupName)
	{
		logger.debug("Creating grouped message batch entity from message batch with group {}", groupName);
		GroupedMessageBatchEntity entity;
		try
		{
			entity = new GroupedMessageBatchEntity(batch, instanceUuid, groupName);
		}
		catch (IOException e)
		{
			CompletableFuture<Void> error = new CompletableFuture<>();
			error.completeExceptionally(e);
			return error;
		}

		CompletableFuture<Void> future = new AsyncOperator<Void>(semaphore).getFuture(() -> doWriteGroupedMessage(entity));
		try {
			Collection<StoredMessageBatch> batches = entity.toStoredGroupMessageBatch().toStoredMessageBatches();

			for (StoredMessageBatch el : batches) {
				future = future.thenComposeAsync(r ->  {
					try {
						return doWriteMessage(new DetailedMessageBatchEntity(el, instanceUuid), true);
					} catch (IOException e) {
						logger.error("Could not save batch {}", el.getId());
						return new CompletableFuture<>();
					}
				});
			}
		} catch (IOException | CradleStorageException e) {
			logger.error("Could not get message batches from group message batch: {}", e.getMessage());
		}

		return future;
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
		return future.thenAcceptAsync(e -> {});
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
		logger.debug("Creating detailed message batch entity from message batch with id {}", batch.getId());
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
		return new AsyncOperator<Void>(semaphore).getFuture(() -> doWriteMessage(entity, false));
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
			DetailedTestEventEntity detailedEntity = new DetailedTestEventEntity(event, instanceUuid);
			CompletableFuture<Void> updateMaxDuration;
			try {
				updateMaxDuration =  eventBatchDurationWorker.updateMaxDuration(
						new EventBatchDurationCache.CacheKey(instanceUuid, detailedEntity.getStartDate()),
						Duration.between(detailedEntity.getStartTime(), detailedEntity.getEndTime()).toMillis());
			} catch (CradleStorageException e) {
				logger.error("Could not update max length for event batch with date {}", detailedEntity.getStartDate());
				throw new CradleStorageException("Could not update max length for event batch", e);
			}

			futures.add(storeTimeEvent(detailedEntity));
			futures.add(updateMaxDuration);
			futures.add(storeDateTime(new DateTimeEventEntity(event, instanceUuid)));
			futures.add(storeChildrenDates(new ChildrenDatesEventEntity(event, instanceUuid)));
		}
		catch (IOException | CradleStorageException e)
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
		TimeMessageOperator tmOperator = ops.getTimeMessageOperator();
		TimeMessageConverter converter = ops.getTimeMessageConverter();

		return timeRelation == TimeRelation.BEFORE
				? selectExecutor.executeSingleRowResultQuery(
						() -> tmOperator.getNearestMessageBefore(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs),
						converter, "getting nearest message time before " + timestamp)

				: selectExecutor.executeSingleRowResultQuery(
						() -> tmOperator.getNearestMessageAfter(instanceUuid, streamName, messageDateTime.toLocalDate(),
								direction.getLabel(), messageDateTime.toLocalTime(), readAttrs),
						converter, "getting nearest message time after " + timestamp);
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
		String dtQueryInfo = format("getting date/time for test event '%s'", id);
		CompletableFuture<DateTimeEventEntity> future =  selectExecutor.executeSingleRowResultQuery(
						() -> ops.getTestEventOperator().get(instanceUuid, id.toString(), readAttrs),
						ops.getDateTimeEventEntityConverter(), dtQueryInfo);
		return future.thenCompose(dtEntity ->
		{
			if (dtEntity == null)
				return CompletableFuture.completedFuture(null);

			String steQueryInfo = format("getting full test event by id '%s'", id);
			return selectExecutor.executeSingleRowResultQuery(() -> ops.getTimeTestEventOperator()
							.get(instanceUuid, dtEntity.getStartDate(), dtEntity.getStartTime(), id.toString(), readAttrs),
					ops.getDetailedTestEventConverter(), steQueryInfo)
					.thenApply(entity ->
					{
						try
						{
							return entity != null ? entity.toStoredTestEventWrapper(objectsFactory) : null;
						}
						catch (Exception error)
						{
							throw new CompletionException("Error while converting data into test event", error);
						}
					});
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
		String queryInfo = "getting messages filtered by " + filter;
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
	protected Iterable<StoredGroupMessageBatch> doGetGroupedMessageBatches(String groupName, Instant from, Instant to)
			throws IOException
	{
		try
		{
			return doGetGroupedMessageBatchesAsync(groupName, from, to).get();
		}
		catch (Exception e)
		{
			throw new IOException(format("Error while getting message batches grouped by %s between %s and %s",
					groupName, from, to), e);
		}
	}

	@Override
	protected CompletableFuture<Iterable<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter)
	{
		String queryInfo = "getting message batches filtered by "+filter;
		return doGetDetailedMessageBatchEntities(filter, queryInfo)
				.thenApply(it -> new StoredMessageBatchAdapter(it, pagingSupplies, ops.getMessageBatchConverter(),
						queryInfo, filter == null ? 0 : filter.getLimit()));
	}

	@Override
	protected CompletableFuture<Iterable<StoredGroupMessageBatch>> doGetGroupedMessageBatchesAsync(String groupName,
																								   Instant from, Instant to)
	{
		String queryInfo = format("fetching grouped message batches by group '%s' between %s and %s", groupName,
				from, to);
		return doGetGroupedMessageBatchEntities(groupName, from, to, queryInfo)
				.thenApplyAsync(it -> new GroupedMessageBatchAdapter(it, pagingSupplies,
						ops.getGroupedMessageBatchConverter(), queryInfo, from, to));
	}

	private CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> doGetDetailedMessageBatchEntities(
			StoredMessageFilter filter, String queryInfo)
	{
		MessageBatchOperator mbOp = ops.getMessageBatchOperator();
		TimeMessageOperator tmOp = ops.getTimeMessageOperator();
		return selectExecutor.executeMultiRowResultQuery(
				() -> mbOp.filterMessages(instanceUuid, filter, mbOp, tmOp, readAttrs),
				ops.getMessageBatchConverter(), queryInfo);
	}

	private CompletableFuture<MappedAsyncPagingIterable<GroupedMessageBatchEntity>> doGetGroupedMessageBatchEntities(
			String groupName, Instant from, Instant to, String queryInfo)
	{
		GroupedMessageBatchOperator gmOp = ops.getGroupedMessageBatchOperator();
		LocalDateTime ldtFrom = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET);
		LocalDateTime ldtTo = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		return selectExecutor.executeMultiRowResultQuery(
				() -> gmOp.getByTimeRange(instanceUuid, groupName, ldtFrom.toLocalDate(), ldtFrom.toLocalTime(),
						ldtTo.toLocalDate(), ldtTo.toLocalTime(), readAttrs),
				ops.getGroupedMessageBatchConverter(), queryInfo);
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
		TestEventsQueryParams params = getAdjustedQueryParams(parentId, from, to);

		String queryInfo = String.format("get %s from range %s..%s",
				parentId == null ? "root" : "child test events of '" + parentId + "'", from, to);

		return selectExecutor.executeMultiRowResultQuery(() ->
						ops.getTimeTestEventOperator().getTestEvents(
								instanceUuid,
								params.getFromDate(),
								params.getFromTime(),
								null,
								params.getToTime(),
								params.getParentId(),
								readAttrs),
						ops.getTestEventConverter(), queryInfo)
				.thenApply(r -> new TestEventDataIteratorAdapter(r, objectsFactory, pagingSupplies,
						ops.getTestEventConverter(), from, queryInfo));
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

				return selectExecutor.executeMultiRowResultQuery(() ->
								ops.getTimeTestEventOperator().getTestEvents(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												order,
												readAttrs),
								ops.getTestEventConverter(), queryInfo)
								.thenApply(r -> new TestEventDataIteratorAdapter(r, objectsFactory, pagingSupplies,
										ops.getTestEventConverter(), from, queryInfo));

		});
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventWrapper>> doGetTestEventsFromIdAsync(StoredTestEventId parentId, StoredTestEventId fromId, Instant to) {

		return getEventTimestampAndThenCompose(fromId, from -> {

				TestEventsQueryParams params = new TestEventsQueryParams(parentId, fromId, from, to);
				String queryInfo = String.format("get test events starting with id %s and parentId %s from range %s..%s", fromId, parentId, from, to);

				return selectExecutor.executeMultiRowResultQuery(() ->
										ops.getTimeTestEventOperator().getTestEvents(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												params.getParentId(),
												readAttrs),
										ops.getTestEventConverter(), queryInfo)
									.thenApply(r -> new TestEventDataIteratorAdapter(r, objectsFactory, pagingSupplies,
												ops.getTestEventConverter(), from, queryInfo));
		});
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsFromIdMetadataAsync(StoredTestEventId fromId, Instant to, Order order) {

		return getEventTimestampAndThenCompose(fromId, from -> {

				TestEventsQueryParams params = new TestEventsQueryParams(fromId, from, to, order);
				String queryInfo = String.format("get test events' metadata starting with id %s from range %s..%s", fromId, from, to);

				return selectExecutor.executeMultiRowResultQuery(() ->
								ops.getTimeTestEventOperator().getTestEventsMetadata(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												order,
												readAttrs),
								ops.getTestEventMetadataConverter(), queryInfo)
								.thenApply(r -> new TestEventMetadataIteratorAdapter(r, pagingSupplies,
										ops.getTestEventMetadataConverter(),
										from,
										queryInfo));
		});
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsFromIdMetadataAsync(StoredTestEventId parentId, StoredTestEventId fromId, Instant to) {

		return getEventTimestampAndThenCompose(fromId, from -> {

				TestEventsQueryParams params = new TestEventsQueryParams(parentId, fromId, from, to, null, 0);
				String queryInfo = String.format("get test events' metadata starting with id %s and parentId %s from range %s..%s", fromId, parentId, from, to);

				return selectExecutor.executeMultiRowResultQuery(() ->
								ops.getTimeTestEventOperator().getTestEventsMetadata(
												instanceUuid,
												params.getFromDate(),
												params.getFromTime(),
												params.getFromId(),
												params.getToTime(),
												params.getParentId(),
												readAttrs),
								ops.getTestEventMetadataConverter(), queryInfo)
						.thenApply(r -> new TestEventMetadataIteratorAdapter(r, pagingSupplies,
						ops.getTestEventMetadataConverter(),
						from,
						queryInfo));
		});
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsMetadataAsync(
			StoredTestEventId parentId, Instant from, Instant to) throws CradleStorageException
	{
		TestEventsQueryParams params = getAdjustedQueryParams(parentId, from, to);

		String queryInfo = String.format("get %s from range %s..%s",
				parentId == null ? "root" : "child test events' metadata of '" + parentId + "'", from, to);

		return selectExecutor.executeMultiRowResultQuery(() ->
						ops.getTimeTestEventOperator().getTestEventsMetadata(
								instanceUuid,
								params.getFromDate(),
								params.getFromTime(),
								null,
								params.getToTime(),
								params.getParentId(),
								readAttrs),
						ops.getTestEventMetadataConverter(), queryInfo)
				.thenApply(r -> new TestEventMetadataIteratorAdapter(r, pagingSupplies,
						ops.getTestEventMetadataConverter(),
						from,
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
		TestEventsQueryParams params = getAdjustedQueryParams(null, from, to);
		String queryInfo = String.format("get test events from range %s..%s", from, to);

		return selectExecutor.executeMultiRowResultQuery(
				() -> ops.getTimeTestEventOperator().getTestEvents(instanceUuid,
						params.getFromDate(), params.getFromTime(), params.getToTime(), readAttrs),
				ops.getTestEventConverter(), queryInfo)
				.thenApply(entity -> new TestEventDataIteratorAdapter(entity, objectsFactory, pagingSupplies,
						ops.getTestEventConverter(), from, queryInfo));
	}

	@Override
	protected CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsMetadataAsync(Instant from,
			Instant to) throws CradleStorageException
	{
		TestEventsQueryParams params = getAdjustedQueryParams(null, from, to);
		String queryInfo = String.format("get test events' metadata from range %s..%s", from, to);

		return selectExecutor.executeMultiRowResultQuery(() ->
						ops.getTimeTestEventOperator().getTestEventsMetadata(
								instanceUuid,
								params.getFromDate(),
								params.getFromTime(),
								null,
								params.getToTime(),
								Order.DIRECT,
								readAttrs),
						ops.getTestEventMetadataConverter(), queryInfo)
				.thenApply(entity ->
						new TestEventMetadataIteratorAdapter(entity,
								pagingSupplies,
								ops.getTestEventMetadataConverter(),
								from, queryInfo));
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
	protected Collection<Instant> doGetRootTestEventsDates() throws IOException
	{
		return doGetTestEventsDates(new StoredTestEventId(ROOT_EVENT_PARENT_ID));
	}

	@Override
	protected Collection<Instant> doGetTestEventsDates(StoredTestEventId parentId) throws IOException
	{
		DateEventEntityConverter converter = ops.getDateEventEntityConverter();
		String queryInfo = format("getting %s event dates",
				ROOT_EVENT_PARENT_ID.equals(parentId.toString()) ? "root" : "children of " + parentId);
		CompletableFuture<PagedIterator<DateEventEntity>> future = selectExecutor.executeMultiRowResultQuery(
				() -> ops.getTestEventChildrenDatesOperator().get(instanceUuid, parentId.toString(),
						readAttrs), converter, queryInfo)
				.thenApply(rows -> new PagedIterator<>(rows, pagingSupplies, converter, queryInfo));

		try
		{
			Collection<Instant> result = new TreeSet<>();
			future.get().forEachRemaining(entity ->
					result.add(entity.getStartDate().atStartOfDay(TIMEZONE_OFFSET).toInstant()));
			return result;
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new IOException("Error occurred while "+queryInfo, e);
		}
	}

	/*
		Get max batch length for current partition,
		adjusts query params accordingly
	 */
	private TestEventsQueryParams getAdjustedQueryParams (StoredTestEventId parentId, Instant from, Instant to) throws CradleStorageException {
		long maxBatchDurationMillis = eventBatchDurationWorker.getMaxDuration(
				new EventBatchDurationCache.CacheKey(instanceUuid, LocalDateTime.ofInstant(from, TIMEZONE_OFFSET).toLocalDate()));


		return new TestEventsQueryParams(parentId, from, to, maxBatchDurationMillis);
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
	
	
	private CompletableFuture<Void> doWriteMessage(DetailedMessageBatchEntity entity, boolean rawMessage)
	{
		logger.trace("Executing message batch storing query");
		MessageBatchOperator op =
				rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
		return op.writeMessageBatch(entity, writeAttrs).thenAcceptAsync(r->{});
	}
	
	private CompletableFuture<Void> doWriteGroupedMessage(GroupedMessageBatchEntity entity)
	{
		logger.trace("Executing grouped message batch storing query");
		return ops.getGroupedMessageBatchOperator().writeMessageBatch(entity, writeAttrs);
	}
	
	private CompletableFuture<DetailedMessageBatchEntity> readMessageBatchEntity(StoredMessageId messageId,
			boolean rawMessage)
	{
		MessageBatchOperator op = rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
		return selectExecutor.executeSingleRowResultQuery(
				() -> CassandraMessageUtils.getMessageBatch(messageId, op, instanceUuid, readAttrs),
				ops.getMessageBatchConverter(), "getting message batch by id " + messageId);
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
		String queryInfo = format("getting first message for stream '%s' and direction '%s'", streamName, direction);
		CompletableFuture<Row> future = selectExecutor.executeSingleRowResultQuery(
				() -> op.getFirstIndex(instanceUuid, streamName, direction.getLabel(), readAttrs),
				r -> r, queryInfo);
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
		CompletableFuture<Row> future = selectExecutor.executeSingleRowResultQuery(
								() -> op.getLastIndex(instanceUuid, streamName, direction.getLabel(), readAttrs),
								r -> r, queryInfo);
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

		public TestEventsQueryParams(StoredTestEventId parentId, StoredTestEventId fromId, Instant from, Instant to, Order order, long adjustMillis)
				throws CradleStorageException
		{
			var original = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET);
			var adjusted = LocalDateTime.ofInstant(from.minusMillis(adjustMillis), TIMEZONE_OFFSET);

			// Adjustment took us to previous partition, get start of current partition
			if (!original.toLocalDate().equals(adjusted.toLocalDate())) {
				adjusted = original.toLocalDate().atStartOfDay();
			}

			this.fromDateTime = adjusted;
			this.toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
			this.parentId = (parentId == null) ? ROOT_EVENT_PARENT_ID : parentId.toString();
			this.fromId = (fromId == null) ? null : fromId.getId();
			this.order = order;

			checkTimeBoundaries(fromDateTime, toDateTime, from, to);
		}

		public TestEventsQueryParams(StoredTestEventId fromId, Instant from, Instant to, Order order)
				throws CradleStorageException
		{
			this (null, fromId, from, to, order, 0);
		}

		public TestEventsQueryParams(StoredTestEventId parentId, Instant from, Instant to, Long adjustMillis)
				throws CradleStorageException
		{
			this (parentId, null, from, to, Order.DIRECT, adjustMillis);
		}

		public TestEventsQueryParams(StoredTestEventId parentId, StoredTestEventId fromId, Instant from, Instant to)
				throws CradleStorageException
		{
			this(parentId, fromId, from, to, Order.DIRECT, 0);
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
