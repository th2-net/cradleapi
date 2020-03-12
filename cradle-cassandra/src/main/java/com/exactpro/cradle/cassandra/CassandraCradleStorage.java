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
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.CradleStream;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.StoredReportBuilder;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.utils.BatchStorageException;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.CradleUtils;
import com.exactpro.cradle.utils.JsonMarshaller;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private final Object messageWritingMonitor = new Object(),
			reportWritingMonitor = new Object();
	private UUID prevBatchId,
			prevReportId;
	private MessageBatch currentBatch;
	private Instant lastBatchFlush;
	private final QueryExecutor exec;
	private MessagesLinker messagesReportsLinker;
	private final ScheduledExecutorService batchFlusher;

	private JsonMarshaller<String> streamMarshaller = new JsonMarshaller<>();

	public CassandraCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		this.connection = connection;
		this.settings = settings;
		this.exec = new QueryExecutor(connection.getSession(), settings.getTimeout());
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
			new TablesCreator(exec, settings).createAll();
			
			currentBatch = new MessageBatch(BATCH_MESSAGES_LIMIT);
			batchFlusher.scheduleWithFixedDelay(new BatchIdleFlusher(BATCH_IDLE_LIMIT), BATCH_IDLE_LIMIT, BATCH_IDLE_LIMIT, TimeUnit.MILLISECONDS);
			UUID instanceId = getInstanceId(instanceName);
			
			messagesReportsLinker = new MessagesLinker(exec, 
					settings.getKeyspace(), settings.getReportMsgsLinkTableName(), REPORT_ID, instanceId);
			
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
			prevBatchId = getExtremeRecordId(ExtremeFunction.MAX, settings.getMessagesTableName());
		
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

	protected UUID getExtremeRecordId(ExtremeFunction extremeFunForTimestamp, String tableName)
	{
		// check MAX/MIN timestamp in table, if table is empty = null
		Select selectTimestamp = selectFrom(settings.getKeyspace(), tableName)
				.function(extremeFunForTimestamp.getValue(), Selector.column(TIMESTAMP))
				.as(TIMESTAMP)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
				.allowFiltering();
		
		Row resultRowWithTimestamp = connection.getSession().execute(selectTimestamp.asCql()).one();
		Instant timestamp = resultRowWithTimestamp.get(TIMESTAMP, GenericType.INSTANT);
		if (timestamp != null)
		{
			Select selectId = selectFrom(settings.getKeyspace(), tableName)
					.column(ID)
					.whereColumn(TIMESTAMP).isEqualTo(literal(timestamp))
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
		
		synchronized (reportWritingMonitor)
		{
			if (prevReportId == null)
				prevReportId = getExtremeRecordId(ExtremeFunction.MAX, settings.getReportsTableName());
			
			UUID id = UUID.randomUUID();
			Insert insert = insertInto(settings.getKeyspace(), settings.getReportsTableName())
					.value(ID, literal(id))
					.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
					.value(NAME, literal(report.getName()))
					.value(TIMESTAMP, literal(report.getTimestamp()))
					.value(SUCCESS, literal(report.isSuccess()))
					.value(COMPRESSED, literal(toCompress))
					.value(CONTENT, literal(ByteBuffer.wrap(reportContent)))
					.value(PREV_ID, literal(prevReportId))
					.ifNotExists();
			exec.executeQuery(insert.asCql());
			
			prevReportId = id;
			
			return id.toString();
		}
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
	public List<String> storeReportMessagesLink(String reportId, Set<StoredMessageId> messagesIds) throws IOException
	{
		return messagesReportsLinker.linkIdToMessages(reportId, messagesIds);
	}

	@Override
	public String getReportIdByMessageId(String messageId) throws IOException
	{
		return messagesReportsLinker.getLinkedIdByMessageId(messageId);
	}

	@Override
	public List<String> getMessageIdsByReportId(String reportId) throws IOException
	{
		return messagesReportsLinker.getMessageIdsByLinkedId(reportId);
	}

	@Override
	public boolean doMessagesRelatedToReportExist(String reportId) throws IOException
	{
		return messagesReportsLinker.doMessagesLinkedToIdExist(reportId);
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
	

	@Override
	public StoredReport getReport(String id) throws IOException
	{
		Select selectFrom = selectFrom(settings.getKeyspace(), settings.getReportsTableName())
				.column(NAME)
				.column(TIMESTAMP)
				.column(SUCCESS)
				.column(COMPRESSED)
				.column(CONTENT)
				.whereColumn(ID).isEqualTo(literal(UUID.fromString(id)))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
				.allowFiltering();
		
		Row resultRow = connection.getSession().execute(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;

		ByteBuffer reportByteBuffer = resultRow.getByteBuffer(CONTENT);
		byte[] reportContent = reportByteBuffer == null ? new byte[0] : reportByteBuffer.array();
		if (resultRow.getBoolean(COMPRESSED))
		{
			try
			{
				reportContent = CradleUtils.decompressData(reportContent);
			}
			catch (IOException | DataFormatException e)
			{
				throw new IOException(String.format("Could not decompress report contents (ID: '%s') from global " +
						"storage",	id), e);
			}
		}
		
		return new StoredReportBuilder().id(id)
				.name(resultRow.getString(NAME))
				.timestamp(resultRow.getInstant(TIMESTAMP))
				.success(resultRow.getBoolean(SUCCESS))
				.content(reportContent)
				.build();
	}


	public String getNextReportId(String reportId)
	{
		return getRecordId(settings.getReportsTableName(), PREV_ID, reportId, ID);
	}

	public String getPrevReportId(String reportId)
	{
		return getRecordId(settings.getReportsTableName(), ID, reportId, PREV_ID);
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
