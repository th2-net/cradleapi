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
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
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
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.iterators.*;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.TestEvent;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventFilter;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.*;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);
	
	private final String keyspace;
	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	private final CassandraSemaphore semaphore;
	
	private CassandraOperators ops;
	
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;
	
	private QueryExecutor exec;
	
	private IntervalsWorker intervalsWorker;
	private PageInfo currentPage;
	
	public CassandraCradleStorage(String book, String keyspace, CassandraConnection connection, CassandraStorageSettings settings)
	{
		super(book);
		this.keyspace = keyspace;
		this.connection = connection;
		this.settings = settings;
		this.semaphore = new CassandraSemaphore(settings.getMaxParallelQueries());
	}
	
	
	public String getKeyspace()
	{
		return keyspace;
	}
	
	String init(boolean prepareStorage) throws CradleStorageException
	{
		connectToCassandra();

		try
		{
			exec = new QueryExecutor(connection.getSession(),
					settings.getTimeout(), settings.getWriteConsistencyLevel(), settings.getReadConsistencyLevel());

			if (prepareStorage)
			{
				logger.info("Creating/updating schema...");
				createTables();
				logger.info("All needed tables created");
			}
			else
				logger.info("Schema creation/update skipped");

			CassandraDataMapper dataMapper = new CassandraDataMapperBuilder(connection.getSession()).build();
			ops = createOperators(dataMapper, settings);
			
			Duration timeout = Duration.ofMillis(settings.getTimeout());
			int resultPageSize = settings.getResultPageSize();
			writeAttrs = builder -> builder.setConsistencyLevel(settings.getWriteConsistencyLevel())
					.setTimeout(timeout);
			readAttrs = builder -> builder.setConsistencyLevel(settings.getReadConsistencyLevel())
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			strictReadAttrs = builder -> builder.setConsistencyLevel(ConsistencyLevel.ALL)
					.setTimeout(timeout)
					.setPageSize(resultPageSize);
			
			intervalsWorker = new CassandraIntervalsWorker(semaphore, null, writeAttrs, readAttrs, ops.getIntervalOperator());
			
			currentPage = findCurrentPage();
		}
		catch (IOException e)
		{
			throw new CradleStorageException("Could not initialize storage", e);
		}
	}
	
	
	@Override
	public PageInfo getCurrentPage()
	{
		return currentPage;
	}
	
	
	@Override
	protected void doDispose() throws CradleStorageException
	{
		if (connection.isRunning())
		{
  		logger.info("Disconnecting from Cassandra...");
  		try
  		{
  			connection.stop();
  		}
  		catch (Exception e)
  		{
  			throw new CradleStorageException("Error while closing Cassandra connection", e);
  		}
		}
		else
			logger.info("Already disconnected from Cassandra");
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
		//TODO: implement
		return null;
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
	protected void doUpdateEventStatus(StoredTestEvent event, boolean success) throws IOException
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
	protected CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEvent event, boolean success)
	{
		String id = event.getId().toString(),
				parentId = event.getParentId() != null ? event.getParentId().toString() : null;
		LocalDateTime ldt = LocalDateTime.ofInstant(event.getStartTimestamp(), TIMEZONE_OFFSET);
		LocalDate ld = ldt.toLocalDate();
		LocalTime lt = ldt.toLocalTime();

		CompletableFuture<AsyncResultSet> result1 = new AsyncOperator<AsyncResultSet>(semaphore)
				.getFuture(() -> ops.getTestEventOperator().updateStatus(null, id, success, writeAttrs)),
				result2 = new AsyncOperator<AsyncResultSet>(semaphore)
						.getFuture(() -> ops.getTimeTestEventOperator().updateStatus(null, ld, lt, id, success, writeAttrs));
		CompletableFuture<AsyncResultSet> result3;
		if (parentId != null)
			result3 = new AsyncOperator<AsyncResultSet>(semaphore)
					.getFuture(() -> ops.getTestEventChildrenOperator().updateStatus(null, parentId, ld, lt, id, success, writeAttrs));
		else
			result3 = new AsyncOperator<AsyncResultSet>(semaphore)
					.getFuture(() -> ops.getRootTestEventOperator().updateStatus(null, ld, lt, id, success, writeAttrs));
		return CompletableFuture.allOf(result1, result2, result3);
	}
	

	@Override
	protected StoredMessage doGetMessage(StoredMessageId id, PageId pageId) throws IOException
	{
		try
		{
			return doGetMessageAsync(id, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message "+id, e);
		}
	}

	@Override
	protected CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id, PageId pageId)
	{
		return readMessage(id, true);
	}

	@Override
	protected Collection<StoredMessage> doGetMessageBatch(StoredMessageId id, PageId pageId) throws IOException
	{
		try
		{
			return doGetMessageBatchAsync(id, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting message batch "+id, e);
		}
	}

	@Override
	protected CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id, PageId pageId)
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
		return doGetDetailedMessageBatchEntities(filter)
				.thenApply(it -> new StoredMessageBatchAdapter(it, objectsFactory, filter == null ? 0 : filter.getLimit()));
	}
	
	
	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, PageId pageId) throws IOException
	{
		//TODO: implement
		return 0;
	}
	
	@Override
	protected Collection<String> doGetSessionAliases(PageId pageId) throws IOException
	{
		List<String> result = new ArrayList<>();
		for (StreamEntity entity : ops.getMessageBatchOperator().getStreams(readAttrs))
		{
			//if (instanceUuid.equals(entity.getInstanceId()))
				result.add(entity.getStreamName());
		}
		result.sort(null);
		return result;
	}
	
	
	@Override
	protected StoredTestEvent doGetTestEvent(StoredTestEventId id, PageId pageId) throws IOException
	{
		//TODO: implement
		try
		{
			return doGetTestEventAsync(id, pageId).get();
		}
		catch (Exception e)
		{
			throw new IOException("Could not get test event", e);
		}
	}

	@Override
	protected CompletableFuture<StoredTestEvent> doGetTestEventAsync(StoredTestEventId id, PageId pageId)
	{
		//TODO: implement
		return null;
	}
	
	
	@Override
	protected Iterable<StoredTestEvent> doGetTestEvents(StoredTestEventFilter filter) throws CradleStorageException, IOException
	{
		//TODO: implement
		try
		{
			return doGetTestEventsAsync(filter).get();
		}
		catch (Exception e)
		{
			throw new IOException("Error while getting test events filtered by "+filter, e);
		}
	}
	
	@Override
	protected CompletableFuture<Iterable<StoredTestEvent>> doGetTestEventsAsync(StoredTestEventFilter filter) throws CradleStorageException, IOException
	{
		//TODO: implement
		return null;
	}
	
	
	@Override
	public IntervalsWorker getIntervalsWorker()
	{
		return intervalsWorker;
	}
	
	
	protected void connectToCassandra()
	{
		if (!connection.isRunning())
		{
  		logger.info("Connecting to Cassandra...");
  		try
  		{
  			connection.start();
  			logger.info("Connected to Cassandra");
  		}
  		catch (Exception e)
  		{
  			throw new CradleStorageException("Could not open Cassandra connection", e);
  		}
		}
		else
			logger.info("Already connected to Cassandra");
	}
	
	protected void createTables() throws IOException
	{
		new TablesCreator(keyspace, exec, settings).createAll();
	}
	
	protected CassandraOperators createOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		return new CassandraOperators(dataMapper, settings);
	}
	
	protected PageInfo findCurrentPage()
	{
		//TODO: implement
		return null;
	}
	
	protected CassandraStorageSettings getSettings()
	{
		return settings;
	}
	
	protected QueryExecutor getQueryExecutor()
	{
		return exec;
	}
	
	protected CassandraSemaphore getSemaphore()
	{
		return semaphore;
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
