/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.dao.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.MessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.iterators.MessagesIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventsIteratorAdapter;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsParentsLinker;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.CassandraTestEventUtils;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.testevents.TestEventsParentsLinker;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;
import com.exactpro.cradle.utils.TestEventUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
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
	private TestEventsParentsLinker testEventsParentsLinker;
	
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
			
			instanceUuid = getInstanceId(instanceName);
			dataMapper = new CassandraDataMapperBuilder(connection.getSession()).build();
			writeAttrs = builder -> builder.setConsistencyLevel(settings.getWriteConsistencyLevel());
			readAttrs = builder -> builder.setConsistencyLevel(settings.getReadConsistencyLevel());
			strictReadAttrs = builder -> builder.setConsistencyLevel(ConsistencyLevel.ALL);
			
			testEventsMessagesLinker = new CassandraTestEventsMessagesLinker(exec, 
					settings.getKeyspace(), settings.getTestEventMsgsLinkTableName(), TEST_EVENT_ID, instanceUuid);
			testEventsParentsLinker = new CassandraTestEventsParentsLinker(exec, settings.getKeyspace(), settings.getTestEventsParentsLinkTableName(), instanceUuid);
			
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
		logger.debug("Storing data of messages batch {}", batch.getId());
		
		DetailedMessageBatchEntity entity = new DetailedMessageBatchEntity(batch, instanceUuid);
		logger.trace("Executing message batch storing query");
		dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName())
				.write(entity, writeAttrs);
	}
	
	
	@Override
	protected void doStoreTestEventBatch(StoredTestEventBatch batch) throws IOException
	{
		storeTestEventBatchData(batch);
		storeTestEventBatchMetadata(batch);
	}
	
	
	@Override
	protected void doStoreTestEventMessagesLink(StoredTestEventId eventId, Set<StoredMessageId> messagesIds) throws IOException
	{
		linkMessagesTo(messagesIds, settings.getTestEventMsgsLinkTableName(), TEST_EVENT_ID, eventId.toString());
	}
	
	
	@Override
	protected StoredMessage doGetMessage(StoredMessageId id) throws IOException
	{
		StoredMessageBatchId batchId = id.getBatchId();
		MessageBatchEntity entity = dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName())
				.get(instanceUuid, 
						batchId.getStreamName(), 
						batchId.getDirection().getLabel(), 
						batchId.getIndex(),
						readAttrs);
		
		return MessageUtils.bytesToOneMessage(entity.getContent(), entity.isCompressed(), id);
	}
	
	@Override
	protected long doGetLastMessageIndex(String streamName, Direction direction) throws IOException
	{
		//Need to query all nodes in all datacenters to get index that is the latest for sure. so using strictReadAttrs
		return dataMapper.messageBatchOperator(settings.getKeyspace(), settings.getMessagesTableName())
				.getLastIndex(streamName, direction.getLabel(), strictReadAttrs);
	}
	
	@Override
	protected StoredTestEvent doGetTestEvent(StoredTestEventId id) throws IOException
	{
		Select selectFrom = CassandraTestEventUtils.prepareSelect(settings.getKeyspace(), settings.getTestEventsTableName(), instanceUuid, false)
				.whereColumn(ID).isEqualTo(literal(id.getBatchId().toString()));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql(), false).one();
		if (resultRow == null)
			return null;
		
		return CassandraTestEventUtils.toTestEvent(resultRow, id);
	}
	
	
	@Override
	public TestEventsMessagesLinker getTestEventsMessagesLinker()
	{
		return testEventsMessagesLinker;
	}
	
	@Override
	public TestEventsParentsLinker getTestEventsParentsLinker()
	{
		return testEventsParentsLinker;
	}
	
	
	@Override
	protected Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException
	{
		return new MessagesIteratorAdapter(filter, settings.getKeyspace(), settings.getMessagesTableName(), 
				exec, instanceUuid, dataMapper.messageBatchConverter());
	}

	@Override
	protected Iterable<StoredTestEvent> doGetTestEvents(boolean onlyRootEvents) throws IOException
	{
		return new TestEventsIteratorAdapter(exec, settings.getKeyspace(), settings.getTestEventsTableName(), instanceUuid, onlyRootEvents);
	}
	
	
	protected CassandraStorageSettings getSettings()
	{
		return settings;
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
	
	
	private void storeTestEventBatchData(StoredTestEventBatch batch) throws IOException
	{
		logger.debug("Storing data of test events batch {}", batch.getId());
		
		byte[] batchContent = TestEventUtils.serializeTestEvents(batch.getTestEvents());
		boolean toCompress = isNeedCompressTestEventBatch(batchContent);
		if (toCompress)
		{
			try
			{
				logger.trace("Compressing test events batch");
				batchContent = CompressionUtils.compressData(batchContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress test event batch contents (ID: '%s') to save in Cradle", 
						batch.getId().toString()), e);
			}
		}
		
		LocalDateTime storedTimestamp = LocalDateTime.now(TIMEZONE_OFFSET),
				firstTimestamp = LocalDateTime.ofInstant(batch.getFirstStartTimestamp(), TIMEZONE_OFFSET),
				lastTimestamp = LocalDateTime.ofInstant(batch.getLastStartTimestamp(), TIMEZONE_OFFSET);
		
		logger.trace("Executing test events batch storing query");
		StoredTestEventId parentId = batch.getParentId();
		RegularInsert insert = insertInto(settings.getKeyspace(), settings.getTestEventsTableName())
				.value(ID, literal(batch.getId().toString()))
				.value(IS_ROOT, literal(parentId == null))
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(STORED_DATE, literal(storedTimestamp.toLocalDate()))
				.value(STORED_TIME, literal(storedTimestamp.toLocalTime()))
				.value(FIRST_EVENT_DATE, literal(firstTimestamp.toLocalDate()))
				.value(FIRST_EVENT_TIME, literal(firstTimestamp.toLocalTime()))
				.value(LAST_EVENT_DATE, literal(lastTimestamp.toLocalDate()))
				.value(LAST_EVENT_TIME, literal(lastTimestamp.toLocalTime()))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(batchContent)))
				.value(BATCH_SIZE, literal(batch.getTestEventsCount()));
		
		if (parentId != null)
			insert = insert.value(PARENT_ID, literal(parentId.toString()));
		
		exec.executeQuery(insert.asCql(), true);
	}
	
	private void storeTestEventBatchMetadata(StoredTestEventBatch batch) throws IOException
	{
		if (batch.getParentId() == null)
			return;
		
		logger.debug("Storing metadata of test events batch {}", batch.getId());
		Insert insert = insertInto(settings.getKeyspace(), settings.getTestEventsParentsLinkTableName())
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(PARENT_ID, literal(batch.getParentId().toString()))
				.value(BATCH_ID, literal(batch.getId().toString()))
				.value(BATCH_SIZE, literal(batch.getTestEventsCount()));
		exec.executeQuery(insert.asCql(), true);
	}
	
	
	private boolean isNeedCompressTestEventBatch(byte[] batchContentBytes)
	{
		return batchContentBytes.length > TEST_EVENT_BATCH_SIZE_LIMIT_BYTES;
	}
	

	private void linkMessagesTo(Set<StoredMessageId> messagesIds, String linksTable, String linkColumn, String linkedId) throws IOException
	{
		List<String> messagesIdsAsStrings = messagesIds.stream().map(StoredMessageId::toString).collect(toList());
		int msgsSize = messagesIdsAsStrings.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + TEST_EVENTS_MSGS_LINK_MAX_MSGS, msgsSize);
			List<String> curMsgsIds = messagesIdsAsStrings.subList(left, right);
			logger.trace("Executing query to link messages to {} {}", linkColumn, linkedId);
			Insert insert = insertInto(settings.getKeyspace(), linksTable)
					.value(INSTANCE_ID, literal(instanceUuid))
					.value(linkColumn, literal(linkedId))
					.value(MESSAGES_IDS, literal(curMsgsIds));
			exec.executeQuery(insert.asCql(), true);
			left = right - 1;
		}
	}
}