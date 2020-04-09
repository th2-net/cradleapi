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
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.CradleStream;
import com.exactpro.cradle.ReportsMessagesLinker;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.StoredTestEvent;
import com.exactpro.cradle.StreamsMessagesLinker;
import com.exactpro.cradle.TestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.iterators.MessagesIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.ReportsIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventsIteratorAdapter;
import com.exactpro.cradle.cassandra.utils.BatchStorageException;
import com.exactpro.cradle.cassandra.utils.MessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.ReportUtils;
import com.exactpro.cradle.cassandra.utils.TestEventUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.CradleUtils;
import com.exactpro.cradle.utils.JsonMarshaller;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.*;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);

	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;
	
	private UUID instanceUuid;

	private final Object messageWritingMonitor = new Object();
	private MessageBatch currentBatch;
	private Instant lastBatchFlush;
	private QueryExecutor exec;
	private final ScheduledExecutorService batchFlusher;

	private JsonMarshaller<String> streamMarshaller = new JsonMarshaller<>();
	
	private StreamsMessagesLinker streamsMessagesLinker;
	private ReportsMessagesLinker reportsMessagesLinker;
	private TestEventsMessagesLinker testEventsMessagesLinker;
	
	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		this.connection = connection;
		this.settings = settings;
		this.batchFlusher = Executors.newScheduledThreadPool(1, r -> new Thread(r, "BatchFlusher"));
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
			
			currentBatch = new MessageBatch(BATCH_MESSAGES_LIMIT);
			batchFlusher.scheduleWithFixedDelay(new BatchIdleFlusher(BATCH_IDLE_LIMIT), BATCH_IDLE_LIMIT, BATCH_IDLE_LIMIT, TimeUnit.MILLISECONDS);
			instanceUuid = getInstanceId(instanceName);
			
			streamsMessagesLinker = new CassandraStreamsMessagesLinker(exec, settings.getKeyspace(), settings.getStreamMsgsLinkTableName(), STREAM_ID, instanceUuid,
					this);
			reportsMessagesLinker = new CassandraReportsMessagesLinker(exec, 
					settings.getKeyspace(), settings.getReportMsgsLinkTableName(), REPORT_ID, instanceUuid);
			testEventsMessagesLinker = new CassandraTestEventsMessagesLinker(exec, 
					settings.getKeyspace(), settings.getTestEventMsgsLinkTableName(), TEST_EVENT_ID, instanceUuid);
			
			return instanceUuid.toString();
		}
		catch (IOException e)
		{
			throw new CradleStorageException("Could not initialize storage", e);
		}
	}
	
	/**
	 * Disposes resources occupied by storage which means closing of opened connections, flushing all buffers, etc.
	 * @throws BatchStorageException if there was error while flushing message buffer. Exception contains all messages from the buffer.
	 */
	@Override
	public void dispose() throws BatchStorageException
	{
		try
		{
			batchFlusher.shutdown();
			synchronized (messageWritingMonitor)
			{
				if (!currentBatch.isEmpty())
				{
					String batchId = currentBatch.getBatchId();
					storeCurrentBatch();
					logger.debug("Batch with ID {} has been saved in storage", batchId);
				}
			}
		}
		catch (Exception e)
		{
			synchronized (messageWritingMonitor)
			{
				throw new BatchStorageException("Error while storing current messages batch", e, currentBatch.getMessagesList());
			}
		}
		finally
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
	}


	@Override
	protected String queryStreamId(String streamName) throws IOException
	{
		Select select = selectFrom(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.column(ID)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceUuid))
				.whereColumn(NAME).isEqualTo(literal(streamName))
				.allowFiltering();  //Name is not part of the key as stream can be renamed. To query by name need to allow filtering
		Row resultRow = exec.executeQuery(select.asCql()).one();
		if (resultRow != null)
			return resultRow.get(ID, GenericType.UUID).toString();
		return null;
	}
	
	@Override
	protected String doStoreStream(CradleStream stream) throws IOException
	{
		UUID id = Uuids.timeBased();
		Insert insert = insertInto(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.value(ID, literal(id))
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(NAME, literal(stream.getName()))
				.value(STREAM_DATA, literal(streamMarshaller.marshal(stream.getStreamData())))
				.ifNotExists();
		exec.executeQuery(insert.asCql());
		return id.toString();
	}
	
	@Override
	protected void doModifyStream(String id, CradleStream newStream) throws IOException
	{
		UUID uuid = UUID.fromString(id);
		Update update = update(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.setColumn(NAME, literal(newStream.getName()))
				.setColumn(STREAM_DATA, literal(streamMarshaller.marshal(newStream.getStreamData())))
				.whereColumn(ID).isEqualTo(literal(uuid))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceUuid));
		exec.executeQuery(update.asCql());
	}
	
	@Override
	protected void doModifyStreamName(String id, String newName) throws IOException
	{
		UUID uuid = UUID.fromString(id);
		Update update = update(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.setColumn(NAME, literal(newName))
				.whereColumn(ID).isEqualTo(literal(uuid))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceUuid));
		exec.executeQuery(update.asCql());
	}
	
	
	@Override
	public StoredMessageId storeMessage(StoredMessage message) throws IOException
	{
		if (getStreamId(message.getStreamName()) == null)
			throw new IOException("Message must be linked with know stream. Message refers to stream '"+message.getStreamName()+"', which is unknown");
		
		synchronized (messageWritingMonitor)
		{
			if (currentBatch.isFull()) //If error occurred during the last save
				storeCurrentBatch();
			
			if (currentBatch.getBatchId() == null)
			{
				if (message.getId() != null)
					currentBatch.init(message.getId().getId());
				else
					currentBatch.init();
			}
			
			StoredMessageId id;
			if (message.getId() == null)
			{
				id = new CassandraStoredMessageId(currentBatch.getBatchId().toString(), currentBatch.getStoredMessagesCount());
				message.setId(id);
			}
			else
				id = message.getId();
			currentBatch.addMessage(message);
			logger.debug("Message has been added to batch with ID {}", currentBatch.getBatchId());
			
			//All batches will have 1-length this way. Need this because message IDs are generated externally, not by Cradle itself.
			//if (currentBatch.isFull())
				storeCurrentBatch();
			
			return id;
		}
	}
	
	@Override
	public String storeReport(StoredReport report) throws IOException
	{
		byte[] reportContent = report.getContent();
		boolean toCompress = isNeedCompressReport(reportContent);
		if (toCompress)
		{
			try
			{
				reportContent = CradleUtils.compressData(reportContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress contents of report '%s' "
						+ " to save in global storage", report.getName()), e);
			}
		}
		
			
		String id = report.getId() != null ? report.getId() : Uuids.timeBased().toString();
		Insert insert = insertInto(settings.getKeyspace(), settings.getReportsTableName())
				.value(ID, literal(id))
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(STORED, literal(Instant.now()))
				.value(NAME, literal(report.getName()))
				.value(TIMESTAMP, literal(report.getTimestamp()))
				.value(SUCCESS, literal(report.isSuccess()))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(reportContent)))
				.ifNotExists();
		exec.executeQuery(insert.asCql());
		
		return id.toString();
	}
	
	@Override
	public void modifyReport(StoredReport report) throws IOException
	{
		byte[] content = report.getContent();
		boolean toCompress = isNeedCompressReport(content);
		if (toCompress)
		{
			try
			{
				content = CradleUtils.compressData(content);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress updated contents of report '%s' "
						+ " to save in global storage", report.getId()), e);
			}
		}
		
		String id = report.getId();
		Update update = update(settings.getKeyspace(), settings.getReportsTableName())
				.setColumn(NAME, literal(report.getName()))
				.setColumn(TIMESTAMP, literal(report.getTimestamp()))
				.setColumn(SUCCESS, literal(report.isSuccess()))
				.setColumn(COMPRESSED, literal(toCompress))
				.setColumn(CONTENT, literal(ByteBuffer.wrap(content)))
				.whereColumn(ID).isEqualTo(literal(id))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceUuid));
		exec.executeQuery(update.asCql());
	}
	
	@Override
	public String storeTestEvent(StoredTestEvent testEvent) throws IOException
	{
		byte[] content = testEvent.getContent();
		boolean toCompress = isNeedCompressTestEvent(content);
		if (toCompress)
		{
			try
			{
				content = CradleUtils.compressData(content);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress contents of test event '%s' "
						+ " to save in global storage", testEvent.getName()), e);
			}
		}
		
			
		String id = testEvent.getId() != null ? testEvent.getId() : Uuids.timeBased().toString();
		RegularInsert insert = insertInto(settings.getKeyspace(), settings.getTestEventsTableName())
				.value(ID, literal(id))
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(STORED, literal(Instant.now()))
				.value(NAME, literal(testEvent.getName()))
				.value(TYPE, literal(testEvent.getType()))
				.value(START_TIMESTAMP, literal(testEvent.getStartTimestamp()))
				.value(END_TIMESTAMP, literal(testEvent.getEndTimestamp()))
				.value(SUCCESS, literal(testEvent.isSuccess()))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(content)))
				.value(REPORT_ID, literal(testEvent.getReportId()));
		if (!StringUtils.isEmpty(testEvent.getParentId()))
			insert = insert.value(PARENT_ID, literal(testEvent.getParentId()));
		exec.executeQuery(insert.ifNotExists().asCql());
		
		return id.toString();
	}
	
	@Override
	public void modifyTestEvent(StoredTestEvent testEvent) throws IOException
	{
		byte[] content = testEvent.getContent();
		boolean toCompress = isNeedCompressTestEvent(content);
		if (toCompress)
		{
			try
			{
				content = CradleUtils.compressData(content);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress updated contents of test event '%s' "
						+ " to save in global storage", testEvent.getId()), e);
			}
		}
		
		UpdateWithAssignments updateColumns = update(settings.getKeyspace(), settings.getTestEventsTableName())
				.setColumn(NAME, literal(testEvent.getName()))
				.setColumn(TYPE, literal(testEvent.getType()))
				.setColumn(START_TIMESTAMP, literal(testEvent.getStartTimestamp()))
				.setColumn(END_TIMESTAMP, literal(testEvent.getEndTimestamp()))
				.setColumn(SUCCESS, literal(testEvent.isSuccess()))
				.setColumn(COMPRESSED, literal(toCompress))
				.setColumn(CONTENT, literal(ByteBuffer.wrap(content)));
		if (!StringUtils.isEmpty(testEvent.getParentId()))
			updateColumns = updateColumns.setColumn(PARENT_ID, literal(testEvent.getParentId()));
		
		String id = testEvent.getId(),
				reportId = testEvent.getReportId();
		Update update = updateColumns
				.whereColumn(ID).isEqualTo(literal(id))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceUuid))
				.whereColumn(REPORT_ID).isEqualTo(literal(reportId));
		
		exec.executeQuery(update.asCql());
	}

	
	@Override
	public void storeReportMessagesLink(String reportId, Set<StoredMessageId> messagesIds) throws IOException
	{
		linkMessagesTo(messagesIds, settings.getReportMsgsLinkTableName(), REPORT_ID, reportId);
	}
	
	@Override
	public void storeTestEventMessagesLink(String eventId, Set<StoredMessageId> messagesIds) throws IOException
	{
		linkMessagesTo(messagesIds, settings.getTestEventMsgsLinkTableName(), TEST_EVENT_ID, eventId);
	}
	
	
	@Override
	public StoredMessage getMessage(StoredMessageId id) throws IOException
	{
		Select selectFrom = MessageUtils.prepareSelect(settings.getKeyspace(), settings.getMessagesTableName(), instanceUuid)
				.whereColumn(ID).isEqualTo(literal(id.getId()));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		int index = id instanceof CassandraStoredMessageId ? ((CassandraStoredMessageId)id).getMessageIndex() : 0;
		return MessageUtils.toMessage(resultRow, index, this);
	}
	
	@Override
	public StoredReport getReport(String id) throws IOException
	{
		Select selectFrom = ReportUtils.prepareSelect(settings.getKeyspace(), settings.getReportsTableName(), instanceUuid)
				.whereColumn(ID).isEqualTo(literal(id));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return ReportUtils.toReport(resultRow);
	}
	
	@Override
	public StoredTestEvent getTestEvent(String reportId, String id) throws IOException
	{
		Select selectFrom = TestEventUtils.prepareSelect(settings.getKeyspace(), settings.getTestEventsTableName(), instanceUuid)
				.whereColumn(REPORT_ID).isEqualTo(literal(reportId))
				.whereColumn(ID).isEqualTo(literal(id));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return TestEventUtils.toTestEvent(resultRow);
	}
	
	
	@Override
	public StreamsMessagesLinker getStreamsMessagesLinker()
	{
		return streamsMessagesLinker;
	}
	
	@Override
	public ReportsMessagesLinker getReportsMessagesLinker()
	{
		return reportsMessagesLinker;
	}
	
	@Override
	public TestEventsMessagesLinker getTestEventsMessagesLinker()
	{
		return testEventsMessagesLinker;
	}
	
	
	@Override
	public Iterable<StoredMessage> getMessages() throws IOException
	{
		return new MessagesIteratorAdapter(exec, settings.getKeyspace(), settings.getMessagesTableName(), instanceUuid, this);
	}

	@Override
	public Iterable<StoredReport> getReports() throws IOException
	{
		return new ReportsIteratorAdapter(exec, settings.getKeyspace(), settings.getReportsTableName(), instanceUuid);
	}
	
	@Override
	public Iterable<StoredTestEvent> getReportTestEvents(String reportId) throws IOException
	{
		return new TestEventsIteratorAdapter(reportId, 
				exec, settings.getKeyspace(), settings.getTestEventsTableName(), instanceUuid);
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
	
	
	private void storeCurrentBatch() throws IOException
	{
		if (currentBatch.isEmpty())
		{
			logger.warn("No messages to store");
			return;
		}
		
		try
		{
			storeCurrentBatchData();
			storeCurrentBatchMetadata();
			
			lastBatchFlush = Instant.now();
			
			String batchId = currentBatch.getBatchId();
			logger.debug("Batch with ID {} has been saved in storage", batchId);
		}
		finally
		{
			currentBatch.clear();  //This prevents batch ID duplication in case of any error in this method
		}
	}

	private void storeCurrentBatchData() throws IOException
	{
		byte[] batchContent = MessageUtils.serializeMessages(currentBatch.getMessages(), this);
		
		Instant batchStartTimestamp = currentBatch.getTimestamp();
		
		boolean toCompress = isNeedCompressBatchContent(batchContent);
		if (toCompress)
		{
			try
			{
				batchContent = CradleUtils.compressData(batchContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress batch contents (batch ID: '%s'" +
								", timestamp: '%s') to save in storage",
						currentBatch.getBatchId(), batchStartTimestamp), e);
			}
		}
		
		Insert insert = insertInto(settings.getKeyspace(), settings.getMessagesTableName())
				.value(ID, literal(currentBatch.getBatchId()))
				.value(INSTANCE_ID, literal(instanceUuid))
				.value(STORED, literal(Instant.now()))
				.value(DIRECTION, literal(currentBatch.getMessagesDirections().getLabel()))
				.value(TIMESTAMP, literal(batchStartTimestamp))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(batchContent)))
				.ifNotExists();
		exec.executeQuery(insert.asCql());
	}

	private void storeCurrentBatchMetadata() throws IOException
	{
		for (Entry<String, Set<StoredMessageId>> streamMessages : currentBatch.getStreamsMessages().entrySet())
		{
			String streamId = getStreamId(streamMessages.getKey());
			linkMessagesTo(streamMessages.getValue(), settings.getStreamMsgsLinkTableName(), STREAM_ID, streamId);
		}
	}
	
	
	private boolean isNeedCompressBatchContent(byte[] batchContentBytes)
	{
		return batchContentBytes.length > BATCH_SIZE_LIMIT_BYTES;
	}
	
	private boolean isNeedCompressReport(byte[] reportBytes)
	{
		return reportBytes.length > REPORT_SIZE_LIMIT_BYTES;
	}
	
	private boolean isNeedCompressTestEvent(byte[] testEventBytes)
	{
		return testEventBytes.length > TEST_EVENT_SIZE_LIMIT_BYTES;
	}
	

	private void linkMessagesTo(Set<StoredMessageId> messagesIds, String linksTable, String linkColumn, String linkedId) throws IOException
	{
		List<String> messagesIdsAsStrings = messagesIds.stream().map(StoredMessageId::toString).collect(toList());
		int msgsSize = messagesIdsAsStrings.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + REPORT_MSGS_LINK_MAX_MSGS, msgsSize);
			List<String> curMsgsIds = messagesIdsAsStrings.subList(left, right);
			Insert insert = insertInto(settings.getKeyspace(), linksTable)
					.value(INSTANCE_ID, literal(instanceUuid))
					.value(linkColumn, literal(linkedId))
					.value(MESSAGES_IDS, literal(curMsgsIds))
					.ifNotExists();
			exec.executeQuery(insert.asCql());
			left = right - 1;
		}
	}
	

	private class BatchIdleFlusher implements Runnable
	{
		private final long maxAgeMillis;
		
		public BatchIdleFlusher(long maxAgeMillis)
		{
			this.maxAgeMillis = maxAgeMillis;
		}
		
		@Override
		public void run()
		{
			synchronized (messageWritingMonitor)
			{
				if (currentBatch.isEmpty() || (lastBatchFlush != null && Duration.between(lastBatchFlush, Instant.now()).abs().toMillis() < maxAgeMillis))
					return;
				
				try
				{
					logger.debug("Writing messages batch while idle");
					storeCurrentBatch();
				}
				catch (Exception e)
				{
					logger.error("Could not write messages batch while idle", e);
				}
			}
		}
	}
}