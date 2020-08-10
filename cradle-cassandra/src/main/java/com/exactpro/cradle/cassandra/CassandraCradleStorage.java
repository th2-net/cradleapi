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
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.StreamEntity;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageEntity;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.dao.testevents.DetailedTestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildrenOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventOperator;
import com.exactpro.cradle.cassandra.iterators.MessagesIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventsIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventsMetadataIteratorAdapter;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
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
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
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
	
	private UUID instanceUuid;
	private CassandraDataMapper dataMapper;
	private Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs,
			strictReadAttrs;

	private QueryExecutor exec;
	
	private TestEventsMessagesLinker testEventsMessagesLinker;
	
	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		this.connection = connection;
		this.settings = settings;
	}
	
	
	public UUID getInstanceUuid()
	{
		return instanceUuid;
	}
	

	@Override
	protected String doInit(String instanceName) throws CradleStorageException
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
			
			logger.info("Creating/updating schema...");
			new TablesCreator(exec, settings).createAll();
			logger.info("All needed tables created");
			
			instanceUuid = getInstanceId(instanceName);
			dataMapper = new CassandraDataMapperBuilder(connection.getSession()).build();
			Duration timeout = Duration.ofMillis(settings.getTimeout());
			writeAttrs = builder -> builder.setConsistencyLevel(settings.getWriteConsistencyLevel())
					.setTimeout(timeout);
			readAttrs = builder -> builder.setConsistencyLevel(settings.getReadConsistencyLevel())
					.setTimeout(timeout);
			strictReadAttrs = builder -> builder.setConsistencyLevel(ConsistencyLevel.ALL)
					.setTimeout(timeout);
			
			testEventsMessagesLinker = new CassandraTestEventsMessagesLinker(exec, settings.getKeyspace(), 
					settings.getTestEventsMessagesTableName(), settings.getMessagesTestEventsTableName(), instanceUuid);
			
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
		logger.debug("Storing data of message batch {}", batch.getId());
		
		writeMessage(batch, settings.getMessagesTableName());
	}
	
	@Override
	protected void doStoreTimeMessage(StoredMessage message) throws IOException
	{
		TimeMessageEntity timeEntity = new TimeMessageEntity(message, instanceUuid);
		
		TimeMessageOperator op = getTimeMessageOperator();
		logger.trace("Executing time/message storing query");
		op.writeMessage(timeEntity, writeAttrs);
	}
	
	@Override
	protected void doStoreProcessedMessageBatch(StoredMessageBatch batch) throws IOException
	{
		logger.debug("Storing data of processed messages batch {}", batch.getId());
		
		writeMessage(batch, settings.getProcessedMessagesTableName());
	}
	
	
	@Override
	protected void doStoreTestEvent(StoredTestEvent event) throws IOException
	{
		logger.debug("Storing data of test event {}", event.getId());
		
		storeEvent(event);
		storeTimeEvent(event);
		if (event.getParentId() != null)
			storeEventInParent(event);
		
	}
	
	@Override
	protected void doStoreTestEventMessagesLink(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messageIds) throws IOException
	{
		List<String> messageIdsStrings = messageIds.stream().map(StoredMessageId::toString).collect(toList());
		String eventIdString = eventId.toString();
		storeMessagesOfTestEvent(eventIdString, messageIdsStrings);
		storeTestEventOfMessages(messageIdsStrings, eventIdString, batchId);
	}
	
	
	@Override
	protected StoredMessage doGetMessage(StoredMessageId id) throws IOException
	{
		return readMessage(id, settings.getMessagesTableName());
	}
	
	@Override
	protected StoredMessage doGetProcessedMessage(StoredMessageId id) throws IOException
	{
		return readMessage(id, settings.getProcessedMessagesTableName());
	}
	
	@Override
	protected long doGetLastMessageIndex(String streamName, Direction direction) throws IOException
	{
		//Need to query all nodes in all datacenters to get index that is the latest for sure. so using strictReadAttrs
		return getMessageBatchOperator()
				.getLastIndex(instanceUuid, streamName, direction.getLabel(), strictReadAttrs);
	}
	
	@Override
	protected StoredMessageId doGetFirstMessageId(Instant seconds, String streamName, Direction direction) throws IOException
	{
		TimeMessageEntity result = getTimeMessageOperator()
				.getFirstMessage(instanceUuid, seconds, streamName, direction.getLabel(), readAttrs);
		return result != null ? result.createMessageId() : null;
	}
	
	@Override
	protected StoredTestEventWrapper doGetTestEvent(StoredTestEventId id) throws IOException
	{
		TestEventEntity entity = dataMapper.testEventOperator(settings.getKeyspace(), settings.getTestEventsTableName())
				.get(instanceUuid, id.toString(), readAttrs);
		if (entity == null)
			return null;
		
		try
		{
			return entity.toStoredTestEventWrapper();
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Could not get test event", e);
		}
	}
	
	
	@Override
	public TestEventsMessagesLinker getTestEventsMessagesLinker()
	{
		return testEventsMessagesLinker;
	}
	
	
	@Override
	protected Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException
	{
		MessageBatchOperator op = getMessageBatchOperator();
		try
		{
			PagingIterable<DetailedMessageBatchEntity> entities = op.filterMessages(instanceUuid, filter, op, readAttrs);
			return new MessagesIteratorAdapter(filter, entities);
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Error while filtering messages", e);
		}
	}

	@Override
	protected Iterable<StoredTestEventWrapper> doGetRootTestEvents() throws IOException
	{
		return new TestEventsIteratorAdapter(getTestEventOperator().getRootEvents(instanceUuid, readAttrs));
	}
	
	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(StoredTestEventId parentId, Instant from, Instant to) 
			throws CradleStorageException, IOException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		LocalDate fromDate = fromDateTime.toLocalDate(),
				toDate = toDateTime.toLocalDate();
		if (!fromDate.equals(toDate))
			throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+from+"' and '"+to+"'");
		
		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();
		return new TestEventsMetadataIteratorAdapter(getTestEventChildrenOperator().getTestEvents(instanceUuid, parentId.toString(), 
				fromDate, fromTime, toTime, readAttrs));
	}
	
	@Override
	protected Iterable<StoredTestEventMetadata> doGetTestEvents(Instant from, Instant to) throws CradleStorageException, IOException
	{
		LocalDateTime fromDateTime = LocalDateTime.ofInstant(from, TIMEZONE_OFFSET),
				toDateTime = LocalDateTime.ofInstant(to, TIMEZONE_OFFSET);
		LocalDate fromDate = fromDateTime.toLocalDate(),
				toDate = toDateTime.toLocalDate();
		if (!fromDate.equals(toDate))
			throw new CradleStorageException("Left and right boundaries should be of the same date, but got '"+from+"' and '"+to+"'");
		
		LocalTime fromTime = fromDateTime.toLocalTime(),
				toTime = toDateTime.toLocalTime();
		return new TestEventsMetadataIteratorAdapter(getTimeTestEventOperator().getTestEvents(instanceUuid, fromDate, fromTime, toTime, readAttrs));
	}
	
	
	@Override
	protected Collection<String> doGetStreams() throws IOException
	{
		List<String> result = new ArrayList<>();
		for (StreamEntity entity : getMessageBatchOperator().getStreams(readAttrs))
		{
			if (instanceUuid.equals(entity.getInstanceId()))
				result.add(entity.getStreamName());
		}
		result.sort(null);
		return result;
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
	
	
	protected MessageBatchOperator getMessageBatchOperator()
	{
		return dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName());
	}
	
	protected TimeMessageOperator getTimeMessageOperator()
	{
		return dataMapper.timeMessageOperator(settings.getKeyspace(), settings.getTimeMessagesTableName());
	}
	
	protected TestEventOperator getTestEventOperator()
	{
		return dataMapper.testEventOperator(settings.getKeyspace(), settings.getTestEventsTableName());
	}
	
	protected TimeTestEventOperator getTimeTestEventOperator()
	{
		return dataMapper.timeTestEventOperator(settings.getKeyspace(), settings.getTimeTestEventsTableName());
	}
	
	protected TestEventChildrenOperator getTestEventChildrenOperator()
	{
		return dataMapper.testEventChildrenOperator(settings.getKeyspace(), settings.getTestEventsChildrenTableName());
	}
	
	private void writeMessage(StoredMessageBatch batch, String tableName) throws IOException
	{
		DetailedMessageBatchEntity entity = new DetailedMessageBatchEntity(batch, instanceUuid);
		logger.trace("Executing message batch storing query");
		dataMapper.messageBatchOperator(settings.getKeyspace(), tableName)
				.writeMessageBatch(entity, writeAttrs);
	}
	
	private StoredMessage readMessage(StoredMessageId id, String tableName) throws IOException
	{
		MessageBatchOperator op = dataMapper.messageBatchOperator(settings.getKeyspace(), tableName);
		MessageBatchEntity entity = CassandraMessageUtils.getMessageBatch(id, op, instanceUuid, readAttrs);
		if (entity == null)
			return null;
		
		return MessageUtils.bytesToOneMessage(entity.getContent(), entity.isCompressed(), id);
	}
	
	
	private void storeEvent(StoredTestEvent event) throws IOException
	{
		DetailedTestEventEntity entity = new DetailedTestEventEntity(event, instanceUuid);
		logger.trace("Executing test event storing query");
		dataMapper.testEventOperator(settings.getKeyspace(), settings.getTestEventsTableName())
				.write(entity, writeAttrs);
	}
	
	private void storeTimeEvent(StoredTestEvent event) throws IOException
	{
		TimeTestEventEntity timeEntity = new TimeTestEventEntity(event, instanceUuid);
		
		TimeTestEventOperator op = getTimeTestEventOperator();
		logger.trace("Executing time/event storing query");
		op.writeTestEvent(timeEntity, writeAttrs);
	}
	
	private void storeEventInParent(StoredTestEvent event) throws IOException
	{
		TestEventChildEntity entity = new TestEventChildEntity(event, instanceUuid);
		
		TestEventChildrenOperator op = getTestEventChildrenOperator();
		logger.trace("Executing parent/event storing query");
		op.writeTestEvent(entity, writeAttrs);
	}
	
	
	private void storeMessagesOfTestEvent(String eventId, List<String> messageIds) throws IOException
	{
		int msgsSize = messageIds.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + TEST_EVENTS_MSGS_LINK_MAX_MSGS, msgsSize);
			Set<String> curMsgsIds = new HashSet<>(messageIds.subList(left, right));
			logger.trace("Executing query to link messages to {}={}", TEST_EVENT_ID, eventId);
			RegularInsert insert = insertInto(settings.getKeyspace(), settings.getTestEventsMessagesTableName())
					.value(INSTANCE_ID, literal(instanceUuid))
					.value(TEST_EVENT_ID, literal(eventId))
					.value(MESSAGE_IDS, literal(curMsgsIds));
			exec.executeQuery(insert.asCql(), true);
			left = right - 1;
		}
	}
	
	private void storeTestEventOfMessages(List<String> messageIds, String eventId, StoredTestEventId batchId) throws IOException
	{
		String batchIdString = batchId != null ? batchId.toString() : null;
		for (String id : messageIds)
		{
			logger.trace("Executing query to link {}={} with {}={}", TEST_EVENT_ID, eventId, MESSAGE_ID, id);
			RegularInsert insert = insertInto(settings.getKeyspace(), settings.getMessagesTestEventsTableName())
					.value(INSTANCE_ID, literal(instanceUuid))
					.value(MESSAGE_ID, literal(id))
					.value(TEST_EVENT_ID, literal(eventId));
			if (batchIdString != null)
				insert = insert.value(BATCH_ID, literal(batchIdString));
			exec.executeQuery(insert.asCql(), true);
		}
	}
}