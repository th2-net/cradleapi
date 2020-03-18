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
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.CradleStream;
import com.exactpro.cradle.ReportsMessagesLinker;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.StoredTestEvent;
import com.exactpro.cradle.TestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.utils.BatchStorageException;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.ReportUtils;
import com.exactpro.cradle.cassandra.utils.TestEventUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.CradleUtils;
import com.exactpro.cradle.utils.JsonMarshaller;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.*;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);

	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;

	private final Object messageWritingMonitor = new Object();
	private UUID prevBatchId;
	private MessageBatch currentBatch;
	private Instant lastBatchFlush;
	private QueryExecutor exec;
	private final ScheduledExecutorService batchFlusher;

	private JsonMarshaller<String> streamMarshaller = new JsonMarshaller<>();
	
	private ReportsMessagesLinker reportsMessagesLinker;
	private TestEventsMessagesLinker testEventsMessagesLinker;
	
	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		this.connection = connection;
		this.settings = settings;
		this.batchFlusher = Executors.newScheduledThreadPool(1, r -> new Thread(r, "BatchFlusher"));
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
			UUID instanceId = getInstanceId(instanceName);
			
			reportsMessagesLinker = new CassandraReportsMessagesLinker(exec, 
					settings.getKeyspace(), settings.getReportMsgsLinkTableName(), REPORT_ID, instanceId);
			testEventsMessagesLinker = new CassandraTestEventsMessagesLinker(exec, 
					settings.getKeyspace(), settings.getTestEventsMsgsLinkTableName(), TEST_EVENT_ID, instanceId);
			
			return instanceId.toString();
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
					UUID batchId = currentBatch.getBatchId();
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
	protected String queryStreamId(String streamName)
	{
		Select select = selectFrom(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.column(ID)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
				.whereColumn(NAME).isEqualTo(literal(streamName))
				.allowFiltering();
		Row resultRow = connection.getSession().execute(select.asCql()).one();
		if (resultRow != null)
			return resultRow.get(ID, GenericType.UUID).toString();
		return null;
	}
	
	
	@Override
	protected String doStoreStream(CradleStream stream) throws IOException
	{
		UUID id = UUID.randomUUID();
		Insert insert = insertInto(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.value(ID, literal(id))
				.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
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
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())));
		exec.executeQuery(update.asCql());
	}
	
	@Override
	protected void doModifyStreamName(String id, String newName) throws IOException
	{
		UUID uuid = UUID.fromString(id);
		Update update = update(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.setColumn(NAME, literal(newName))
				.whereColumn(ID).isEqualTo(literal(uuid))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())));
		exec.executeQuery(update.asCql());
	}
	
	
	@Override
	public StoredMessageId storeMessage(StoredMessage message) throws IOException
	{
		synchronized (messageWritingMonitor)
		{
			UUID batchId;
			if (currentBatch.isFull()) // if an error occurred during the last save
			{
				batchId = currentBatch.getBatchId();
				storeCurrentBatch();
				logger.debug("Batch with ID {} has been saved in storage", batchId);
			}
			
			if (currentBatch.getBatchId() == null)
				currentBatch.init();
			
			int index = currentBatch.getStoredMessagesCount();
			currentBatch.addMessage(message);
			logger.debug("Message has been added to batch with ID {}", currentBatch.getBatchId());
			
			batchId = currentBatch.getBatchId();
			if (currentBatch.isFull())
			{
				storeCurrentBatch();
				logger.debug("Batch with ID {} has been saved in storage", batchId);
			}
			
			return new CassandraStoredMessageId(batchId.toString(), index);
		}
	}
	
	private void storeCurrentBatch() throws IOException
	{
		if (currentBatch.isEmpty())
		{
			logger.warn("No messages to store");
			return;
		}
		
		storeCurrentBatchData();
		storeCurrentBatchMetadata();
		
		lastBatchFlush = Instant.now();
		
		prevBatchId = currentBatch.getBatchId();
		currentBatch.clear();
	}

	private void storeCurrentBatchData() throws IOException
	{
		byte[] batchContent = serializeStoredMsgs();
		
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
		
		if (prevBatchId == null)
			prevBatchId = getExtremeRecordId(ExtremeFunction.MAX, settings.getMessagesTableName(), TIMESTAMP);
		
		Insert insert = insertInto(settings.getKeyspace(), settings.getMessagesTableName())
				.value(ID, literal(currentBatch.getBatchId()))
				.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
				.value(TIMESTAMP, literal(batchStartTimestamp))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(batchContent)))
				.value(PREV_ID, literal(prevBatchId))
				.ifNotExists();
		exec.executeQuery(insert.asCql());
	}

	private void storeCurrentBatchMetadata() throws IOException
	{
		storeBatchDirectionLink();
		storeBatchStreamsLink();
	}

	private void storeBatchDirectionLink() throws IOException
	{
		Insert insert = insertInto(settings.getKeyspace(), settings.getBatchDirMetadataTableName())
				.value(BATCH_ID, literal(currentBatch.getBatchId()))
				.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
				.value(DIRECTION, literal(currentBatch.getMsgsDirections().getLabel()))
				.ifNotExists();
		
		exec.executeQuery(insert.asCql());
	}

	private void storeBatchStreamsLink() throws IOException
	{
		for (String streamName : currentBatch.getMsgsStreamsNames())
		{
			UUID id = UUID.randomUUID();
			Insert insert = insertInto(settings.getKeyspace(), settings.getBatchStreamsMetadataTableName())
					.value(ID, literal(id))
					.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
					.value(BATCH_ID, literal(currentBatch.getBatchId()))
					.value(STREAM_ID, literal(UUID.fromString(queryStreamId(streamName))))
					.ifNotExists();
			
			exec.executeQuery(insert.asCql());
		}
	}

	protected UUID getExtremeRecordId(ExtremeFunction extremeFunForTimestamp, String tableName, String timestampColumnName)
	{
		// check MAX/MIN timestamp in table, if table is empty = null
		Select selectTimestamp = selectFrom(settings.getKeyspace(), tableName)
				.function(extremeFunForTimestamp.getValue(), Selector.column(timestampColumnName))
				.as(TIMESTAMP)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
				.allowFiltering();
		
		Row resultRowWithTimestamp = connection.getSession().execute(selectTimestamp.asCql()).one();
		Instant timestamp = resultRowWithTimestamp.get(TIMESTAMP, GenericType.INSTANT);
		if (timestamp != null)
		{
			Select selectId = selectFrom(settings.getKeyspace(), tableName)
					.column(ID)
					.whereColumn(timestampColumnName).isEqualTo(literal(timestamp))
					.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
					.allowFiltering();
			
			Row resultRow = connection.getSession().execute(selectId.asCql()).one();
			return resultRow.get(ID, GenericType.UUID);
		}
		
		return null;
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
		
			
		UUID id = Uuids.timeBased();
		Insert insert = insertInto(settings.getKeyspace(), settings.getReportsTableName())
				.value(ID, literal(id))
				.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
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
		
		UUID id = UUID.fromString(report.getId());
		Update update = update(settings.getKeyspace(), settings.getReportsTableName())
				.setColumn(NAME, literal(report.getName()))
				.setColumn(TIMESTAMP, literal(report.getTimestamp()))
				.setColumn(SUCCESS, literal(report.isSuccess()))
				.setColumn(COMPRESSED, literal(toCompress))
				.setColumn(CONTENT, literal(ByteBuffer.wrap(content)))
				.whereColumn(ID).isEqualTo(literal(id))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())));
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
		
			
		UUID id = Uuids.timeBased();
		RegularInsert insert = insertInto(settings.getKeyspace(), settings.getTestEventsTableName())
				.value(ID, literal(id))
				.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
				.value(NAME, literal(testEvent.getName()))
				.value(TYPE, literal(testEvent.getType()))
				.value(START_TIMESTAMP, literal(testEvent.getStartTimestamp()))
				.value(END_TIMESTAMP, literal(testEvent.getEndTimestamp()))
				.value(SUCCESS, literal(testEvent.isSuccess()))
				.value(COMPRESSED, literal(toCompress))
				.value(CONTENT, literal(ByteBuffer.wrap(content)))
				.value(REPORT_ID, literal(UUID.fromString(testEvent.getReportId())));
		if (!StringUtils.isEmpty(testEvent.getParentId()))
			insert = insert.value(PARENT_ID, literal(UUID.fromString(testEvent.getParentId())));
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
				.setColumn(CONTENT, literal(ByteBuffer.wrap(content)))
				.setColumn(REPORT_ID, literal(UUID.fromString(testEvent.getReportId())));
		if (!StringUtils.isEmpty(testEvent.getParentId()))
			updateColumns = updateColumns.setColumn(PARENT_ID, literal(UUID.fromString(testEvent.getParentId())));
		
		UUID id = UUID.fromString(testEvent.getId());
		Update update = updateColumns.whereColumn(ID).isEqualTo(literal(id))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())));
		
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
		linkMessagesTo(messagesIds, settings.getTestEventsMsgsLinkTableName(), TEST_EVENT_ID, eventId);
	}
	
	
	@Override
	public List<StoredMessage> getMessages(String rowId) throws IOException
	{
		Select selectFrom = selectFrom(settings.getKeyspace(), settings.getMessagesTableName())
				.column(ID)
				.column(TIMESTAMP)
				.column(COMPRESSED)
				.column(CONTENT)
				.whereColumn(ID).isEqualTo(literal(UUID.fromString(rowId)))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
				.allowFiltering();
		
		Row resultRow = connection.getSession().execute(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return buildMessagesFromRow(resultRow);
	}
	
	@Override
	public StoredReport getReport(String id) throws IOException
	{
		Select selectFrom = ReportUtils.prepareSelect(settings.getKeyspace(), settings.getReportsTableName(), UUID.fromString(getInstanceId()))
				.whereColumn(ID).isEqualTo(literal(UUID.fromString(id)));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return ReportUtils.toReport(resultRow);
	}
	
	@Override
	public StoredTestEvent getTestEvent(String id) throws IOException
	{
		Select selectFrom = TestEventUtils.prepareSelect(settings.getKeyspace(), settings.getTestEventsTableName(), UUID.fromString(getInstanceId()))
				.whereColumn(ID).isEqualTo(literal(UUID.fromString(id)));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return TestEventUtils.toTestEvent(resultRow);
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
	public Iterable<StoredReport> getReports() throws IOException
	{
		return new CassandraReportsIteratorAdapter(exec, settings.getKeyspace(), settings.getReportsTableName(), UUID.fromString(getInstanceId()));
	}
	
	@Override
	public Iterable<StoredTestEvent> getReportTestEvents(String reportId) throws IOException
	{
		return new CassandraTestEventsIteratorAdapter(UUID.fromString(reportId), 
				exec, settings.getKeyspace(), settings.getTestEventsTableName(), UUID.fromString(getInstanceId()));
	}
	
	
	public String getNextRowId(String batchId)
	{
		return getRecordId(settings.getMessagesTableName(), PREV_ID, batchId, ID);
	}

	public String getPrevRowId(String batchId)
	{
		return getRecordId(settings.getMessagesTableName(), ID, batchId, PREV_ID);
	}

	protected String getRecordId(String tableName, String conditionColumnName, String conditionValue, String resultColumnName)
	{
		Select selectFrom = selectFrom(settings.getKeyspace(), tableName)
				.column(resultColumnName)
				.whereColumn(conditionColumnName).isEqualTo(literal(UUID.fromString(conditionValue)))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
				.allowFiltering();

		Row resultRow = connection.getSession().execute(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;

		UUID id = resultRow.get(resultColumnName, GenericType.UUID);
		if (id == null)
			return null;

		return id.toString();
	}

	protected CassandraStorageSettings getSettings()
	{
		return settings;
	}
	
	
	protected String getPathToReport(Path reportPath)
	{
		return reportPath.toString();
	}
	

	protected UUID getInstanceId(String instanceName) throws IOException
	{
		UUID id;
		Select selectFrom = selectFrom(settings.getKeyspace(), INSTANCES_TABLE_DEFAULT_NAME)
				.column(ID)
				.whereColumn(NAME).isEqualTo(literal(instanceName))
				.allowFiltering();
		
		Row resultRow = connection.getSession().execute(selectFrom.asCql()).one();
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
	
	
	protected List<StoredMessage> buildMessagesFromRow(Row row) throws IOException
	{
		UUID id = row.get(ID, GenericType.UUID);
		Boolean compressed = row.get(COMPRESSED, GenericType.BOOLEAN);
		
		ByteBuffer contentByteBuffer = row.get(CONTENT, GenericType.BYTE_BUFFER);
		byte[] contentBytes = contentByteBuffer.array();
		if (Boolean.TRUE.equals(compressed))
		{
			try
			{
				contentBytes = CradleUtils.decompressData(contentBytes);
			}
			catch (IOException | DataFormatException e)
			{
				throw new IOException(String.format("Could not decompress batch contents (ID: '%s') from global " +
						"storage",	id), e);
			}
		}
		
		return deserializeStoredMsgs(contentBytes, id);
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
	

	protected byte[] serializeStoredMsgs() throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out))
		{
			for (StoredMessage msg : currentBatch.getMessages())
			{
				if (msg == null)  //For case of not full batch
					break;
				StoredMessage copy = new StoredMessage(msg);
				//Replacing stream name with stream ID to be aware of possible stream rename in future
				copy.setStreamName(getStreamId(copy.getStreamName()));
				
				byte[] serializedMsg = SerializationUtils.serialize(copy);
				dos.writeInt(serializedMsg.length);
				dos.write(serializedMsg);
			}
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}

	protected List<StoredMessage> deserializeStoredMsgs(byte[] contentBytes, UUID id) throws IOException
	{
		List<StoredMessage> storedMessages = new ArrayList<>();
		String batchId = id.toString();
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			int index = -1;
			while (dis.available() != 0)
			{
				index++;
				int num = dis.readInt();
				byte[] msg = new byte[num];
				dis.read(msg);
				StoredMessage tempMessage = (StoredMessage) SerializationUtils.deserialize(msg);
				
				//Replacing stream ID obtained from DB with corresponding stream name
				tempMessage.setStreamName(getStreamName(tempMessage.getStreamName()));
				tempMessage.setId(new CassandraStoredMessageId(batchId, index));
				storedMessages.add(tempMessage);
			}
		}
		
		return storedMessages;
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
					.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
					.value(linkColumn, literal(UUID.fromString(linkedId)))
					.value(MESSAGES_IDS, literal(curMsgsIds))
					.ifNotExists();
			exec.executeQuery(insert.asCql());
			left = right - 1;
		}
	}
	

	protected enum ExtremeFunction
	{
		MIN("min"), MAX("max");

		private String value;

		ExtremeFunction(String value)
		{
			this.value = value;
		}

		public String getValue()
		{
			return value;
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
