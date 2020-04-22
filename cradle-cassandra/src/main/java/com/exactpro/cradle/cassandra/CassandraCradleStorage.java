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

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.StreamsMessagesLinker;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.iterators.MessagesIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventsIteratorAdapter;
import com.exactpro.cradle.cassandra.linkers.CassandraStreamsMessagesLinker;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.linkers.CassandraTestEventsParentsLinker;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.CassandraTestEventUtils;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
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
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.*;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);

	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	
	private UUID instanceUuid;

	private QueryExecutor exec;
	
	private StreamsMessagesLinker streamsMessagesLinker;
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
			exec = new QueryExecutor(connection.getSession(), settings.getTimeout());
			
			new TablesCreator(exec, settings).createAll();
			
			instanceUuid = getInstanceId(instanceName);
			streamsMessagesLinker = new CassandraStreamsMessagesLinker(exec, settings.getKeyspace(), settings.getStreamMsgsLinkTableName(), STREAM_NAME, instanceUuid);
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
	public void dispose() throws CradleStorageException
	{
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
	public void storeMessageBatch(StoredMessageBatch batch) throws IOException
	{
		storeMessageBatchData(batch);
		storeMessageBatchMetadata(batch);
	}
	
	
	@Override
	public void storeTestEventBatch(StoredTestEventBatch batch) throws IOException
	{
		storeTestEventBatchData(batch);
		storeTestEventBatchMetadata(batch);
	}
	
	
	@Override
	public void storeTestEventMessagesLink(StoredTestEventId eventId, Set<StoredMessageId> messagesIds) throws IOException
	{
		linkMessagesTo(messagesIds, settings.getTestEventMsgsLinkTableName(), TEST_EVENT_ID, eventId.toString());
	}
	
	
	@Override
	public StoredMessage getMessage(StoredMessageId id) throws IOException
	{
		Select selectFrom = CassandraMessageUtils.prepareSelect(settings.getKeyspace(), settings.getMessagesTableName(), instanceUuid)
				.whereColumn(ID).isEqualTo(literal(id.getBatchId().toString()));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return CassandraMessageUtils.toMessage(resultRow, id);
	}
	
	@Override
	public StoredTestEvent getTestEvent(StoredTestEventId id) throws IOException
	{
		Select selectFrom = CassandraTestEventUtils.prepareSelect(settings.getKeyspace(), settings.getTestEventsTableName(), instanceUuid, false)
				.whereColumn(ID).isEqualTo(literal(id.getBatchId().toString()));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return CassandraTestEventUtils.toTestEvent(resultRow, id);
	}
	
	
	@Override
	public StreamsMessagesLinker getStreamsMessagesLinker()
	{
		return streamsMessagesLinker;
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
	public Iterable<StoredMessage> getMessages() throws IOException
	{
		return new MessagesIteratorAdapter(exec, settings.getKeyspace(), settings.getMessagesTableName(), instanceUuid);
	}

	@Override
	public Iterable<StoredTestEvent> getTestEvents(boolean onlyRootEvents) throws IOException
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
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow != null)
			id = resultRow.get(ID, GenericType.UUID);
		else
		{
			id = UUID.randomUUID();
			Insert insert = insertInto(settings.getKeyspace(), INSTANCES_TABLE_DEFAULT_NAME)
					.value(ID, literal(id))
					.value(NAME, literal(instanceName))
					.ifNotExists();
			exec.executeQuery(insert.asCql());
		}
		
		return id;
	}
	
	
	private void storeMessageBatchData(StoredMessageBatch batch) throws IOException
	{
		byte[] batchContent = MessageUtils.serializeMessages(batch.getMessages());
		
		Instant batchStartTimestamp = batch.getTimestamp();
		
		boolean toCompress = isNeedToCompressMessageBatch(batchContent);
		if (toCompress)
		{
			try
			{
				batchContent = CompressionUtils.compressData(batchContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress message batch contents (ID: '%s') to save in Cradle",
						batch.getId().toString()), e);
			}
		}
		
		Insert insert = insertInto(settings.getKeyspace(), settings.getMessagesTableName())
				.value(ID, literal(batch.getId().toString()))
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(STORED, literal(Instant.now()))
				.value(DIRECTION, literal(batch.getMessagesDirections().getLabel()))
				.value(TIMESTAMP, literal(batchStartTimestamp))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(batchContent)))
				.value(BATCH_SIZE, literal(batch.getMessageCount()));
		exec.executeQuery(insert.asCql());
	}

	private void storeMessageBatchMetadata(StoredMessageBatch batch) throws IOException
	{
		for (Entry<String, Set<StoredMessageId>> streamMessages : batch.getStreamsMessages().entrySet())
			linkMessagesTo(streamMessages.getValue(), settings.getStreamMsgsLinkTableName(), STREAM_NAME, streamMessages.getKey());
	}
	
	
	private void storeTestEventBatchData(StoredTestEventBatch batch) throws IOException
	{
		byte[] batchContent = TestEventUtils.serializeTestEvents(batch.getTestEvents());
		boolean toCompress = isNeedCompressTestEventBatch(batchContent);
		if (toCompress)
		{
			try
			{
				batchContent = CompressionUtils.compressData(batchContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress test event batch contents (ID: '%s') to save in Cradle", 
						batch.getId().toString()), e);
			}
		}
		
		StoredTestEventId parentId = batch.getParentId();
		RegularInsert insert = insertInto(settings.getKeyspace(), settings.getTestEventsTableName())
				.value(ID, literal(batch.getId().toString()))
				.value(IS_ROOT, literal(parentId == null))
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(STORED, literal(Instant.now()))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(batchContent)))
				.value(BATCH_SIZE, literal(batch.getTestEventsCount()));
		if (parentId != null)
			insert = insert.value(PARENT_ID, literal(parentId.toString()));
		
		exec.executeQuery(insert.asCql());
	}
	
	private void storeTestEventBatchMetadata(StoredTestEventBatch batch) throws IOException
	{
		if (batch.getParentId() == null)
			return;
		
		Insert insert = insertInto(settings.getKeyspace(), settings.getTestEventsParentsLinkTableName())
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(PARENT_ID, literal(batch.getParentId().toString()))
				.value(BATCH_ID, literal(batch.getId().toString()))
				.value(BATCH_SIZE, literal(batch.getTestEventsCount()));
		exec.executeQuery(insert.asCql());
	}
	
	
	private boolean isNeedToCompressMessageBatch(byte[] batchContentBytes)
	{
		return batchContentBytes.length > MESSAGE_BATCH_SIZE_LIMIT_BYTES;
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
			Insert insert = insertInto(settings.getKeyspace(), linksTable)
					.value(INSTANCE_ID, literal(instanceUuid))
					.value(linkColumn, literal(linkedId))
					.value(MESSAGES_IDS, literal(curMsgsIds));
			exec.executeQuery(insert.asCql());
			left = right - 1;
		}
	}
}