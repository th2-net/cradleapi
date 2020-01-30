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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.CradleStream;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.CradleUtils;
import com.exactpro.cradle.utils.JsonMarshaller;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
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
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

public class CassandraCradleStorage extends CradleStorage
{
	private Logger logger = LoggerFactory.getLogger(CassandraCradleStorage.class);

	private final CassandraConnection connection;
	private final CassandraStorageSettings settings;

	private final Object writingMonitor = new Object();
	private UUID prevBatchId;
	private UUID prevReportId;
	private MessageBatch currentBatch;
	private Instant lastBatchFlush;
	private final ScheduledExecutorService batchFlusher;

	private JsonMarshaller<String> streamMarshaller = new JsonMarshaller<>();

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
		
		createKeyspace();
		createInstancesTable();
		createStreamsTable();
		createMessagesTable();
		createBatchDirMetadataTable();
		createBatchStreamsMetadataTable();
		createReportsTable();
		createReportMessagesLinkTable();
		currentBatch = new MessageBatch(BATCH_MESSAGES_LIMIT);
		batchFlusher.scheduleWithFixedDelay(new BatchIdleFlusher(BATCH_IDLE_LIMIT), BATCH_IDLE_LIMIT, BATCH_IDLE_LIMIT, TimeUnit.MILLISECONDS);
		return getInstanceId(instanceName).toString();
	}

	@Override
	public void dispose()
	{
		try
		{
			batchFlusher.shutdown();
			synchronized (writingMonitor)
			{
				if (!currentBatch.isEmpty())
				{
					UUID batchId = currentBatch.getBatchId();
					storeCurrentBatch();
					logger.debug("Batch with ID {} has been saved in storage", batchId);
				}
			}
			connection.stop();
		}
		catch (Exception e)
		{
			logger.error("Error while closing Cassandra connection", e);
		}
	}

	@Override
	protected String queryStreamId(String streamName)
	{
		Select select = selectFrom(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.column(ID)
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
		RegularInsert insert = insertInto(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.value(ID, literal(id))
				.value(NAME, literal(stream.getName()))
				.value(STREAM_DATA, literal(streamMarshaller.marshal(stream.getStreamData())));
		executeQuery(insert.asCql());
		return id.toString();
	}
	
	@Override
	protected void doModifyStream(String id, CradleStream newStream) throws IOException
	{
		UUID uuid = UUID.fromString(id);
		Update update = update(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.setColumn(NAME, literal(newStream.getName()))
				.setColumn(STREAM_DATA, literal(streamMarshaller.marshal(newStream.getStreamData())))
				.whereColumn(ID).isEqualTo(literal(uuid));
		executeQuery(update.asCql());
	}
	
	@Override
	protected void doModifyStreamName(String id, String newName)
	{
		UUID uuid = UUID.fromString(id);
		Update update = update(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME)
				.setColumn(NAME, literal(newName))
				.whereColumn(ID).isEqualTo(literal(uuid));
		executeQuery(update.asCql());
	}
	
	
	@Override
	public StoredMessageId storeMessage(StoredMessage message) throws IOException
	{
		synchronized (writingMonitor)
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
		
		RegularInsert insert = insertInto(settings.getKeyspace(), settings.getMessagesTableName())
				.value(ID, literal(currentBatch.getBatchId()))
				.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
				.value(COMPRESSED, literal(toCompress))
				.value(TIMESTAMP, literal(batchStartTimestamp))
				.value(BATCH_CONTENT, literal(ByteBuffer.wrap(batchContent)))
				.value(PREV_BATCH_ID, literal(prevBatchId));
		executeQuery(insert.asCql());
	}

	private void storeCurrentBatchMetadata()
	{
		storeBatchDirectionLink();
		storeBatchStreamsLink();
	}

	private void storeBatchDirectionLink()
	{
		RegularInsert insert = insertInto(settings.getKeyspace(), settings.getBatchDirMetadataTableName())
				.value(BATCH_ID, literal(currentBatch.getBatchId()))
				.value(DIRECTION, literal(currentBatch.getMsgsDirections().getLabel()));

		executeQuery(insert.asCql());
	}

	private void storeBatchStreamsLink()
	{
		for (String streamName : currentBatch.getMsgsStreamsNames())
		{
			UUID id = UUID.randomUUID();
			RegularInsert insert = insertInto(settings.getKeyspace(), settings.getBatchStreamsMetadataTableName())
					.value(ID, literal(id))
					.value(BATCH_ID, literal(currentBatch.getBatchId()))
					.value(STREAM_ID, literal(UUID.fromString(queryStreamId(streamName))));

			executeQuery(insert.asCql());
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
	public String storeReport(Path reportPath, String matrixName) throws IOException
	{
		byte[] reportContent = Files.readAllBytes(reportPath);
		boolean toCompress = isNeedCompressReport(reportContent);
		String reportPathStr = getPathToReport(reportPath);

		if (toCompress)
		{
			try
			{
				reportContent = CradleUtils.compressData(reportContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress report contents (path to report: '%s') "
						+ " to save in global storage", reportPathStr), e);
			}
		}

		if (prevReportId == null)
			prevReportId = getExtremeRecordId(ExtremeFunction.MAX, settings.getReportsTableName());
		
		UUID id = UUID.randomUUID();
		Instant timestamp = Instant.now();
		RegularInsert insert = insertInto(settings.getKeyspace(), settings.getReportsTableName())
				.value(ID, literal(id))
				.value(INSTANCE_ID, literal(UUID.fromString(getInstanceId())))
				.value(TIMESTAMP, literal(timestamp))
				.value(COMPRESSED, literal(toCompress))
				.value(REPORT_PATH, literal(reportPathStr))
				.value(REPORT_CONTENT, literal(ByteBuffer.wrap(reportContent)))
				.value(PREV_REPORT_ID, literal(prevReportId))
				.value(MATRIX_NAME, literal(matrixName));
		executeQuery(insert.asCql());

		prevReportId = id;

		return id.toString();
	}

	@Override
	public List<String> storeReportMessagesLink(String reportId, Set<StoredMessageId> messagesIds)
	{
		List<String> ids = new ArrayList<>();
		List<String> messagesIdsAsStrings = messagesIds.stream().map(StoredMessageId::toString).collect(toList());
		int msgsSize = messagesIdsAsStrings.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + REPORT_MSGS_LINK_MAX_MSGS, msgsSize);
			List<String> curMsgsIds = messagesIdsAsStrings.subList(left, right);
			UUID id = UUID.randomUUID();
			RegularInsert insert = insertInto(settings.getKeyspace(), settings.getReportMsgsLinkTableName())
					.value(ID, literal(id))
					.value(REPORT_ID, literal(UUID.fromString(reportId)))
					.value(MESSAGES_IDS, literal(curMsgsIds));
			executeQuery(insert.asCql());
			ids.add(id.toString());
			left = right - 1;
		}
		return ids;
	}

	@Override
	public String getReportIdByMessageId(String messageId)
	{
		Select selectFrom = selectFrom(settings.getKeyspace(), settings.getReportMsgsLinkTableName())
				.column(REPORT_ID)
				.whereColumn(MESSAGES_IDS).contains(literal(messageId))
				.allowFiltering();

		Row resultRow = connection.getSession().execute(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;

		UUID id = resultRow.get(REPORT_ID, GenericType.UUID);
		if (id == null)
			return null;

		return id.toString();
	}

	@Override
	public List<String> getMessageIdsByReportId(String reportId)
	{
		Select selectFrom = selectFrom(settings.getKeyspace(), settings.getReportMsgsLinkTableName())
				.column(MESSAGES_IDS)
				.whereColumn(REPORT_ID).isEqualTo(literal(UUID.fromString(reportId)))
				.allowFiltering();

		Iterator<Row> resultIterator = connection.getSession().execute(selectFrom.asCql()).iterator();
		List<String> ids = new ArrayList<>();
		while (resultIterator.hasNext())
		{
			List<String> currentMessageIds = resultIterator.next().get(MESSAGES_IDS,
					GenericType.listOf(GenericType.STRING));
			if (currentMessageIds != null)
				ids.addAll(currentMessageIds);
		}
		if (ids.isEmpty())
			return null;

		return ids;
	}

	@Override
	public boolean doMessagesRelatedToReportExist(String reportId)
	{
		Select selectFrom = selectFrom(settings.getKeyspace(), settings.getReportMsgsLinkTableName())
				.column(MESSAGES_IDS)
				.whereColumn(REPORT_ID).isEqualTo(literal(UUID.fromString(reportId)))
				.limit(1)
				.allowFiltering();

		return connection.getSession().execute(selectFrom.asCql()).one() != null;
	}

	@Override
	public List<StoredMessage> getMessages(String rowId) throws IOException
	{
		Select selectFrom = selectFrom(settings.getKeyspace(), settings.getMessagesTableName())
				.column(ID)
				.column(COMPRESSED)
				.column(TIMESTAMP)
				.column(BATCH_CONTENT)
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
		return getRecordId(settings.getMessagesTableName(), PREV_BATCH_ID, batchId, ID);
	}

	public String getPrevRowId(String batchId)
	{
		return getRecordId(settings.getMessagesTableName(), ID, batchId, PREV_BATCH_ID);
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
				.column(TIMESTAMP)
				.column(COMPRESSED)
				.column(REPORT_PATH)
				.column(REPORT_CONTENT)
				.column(MATRIX_NAME)
				.whereColumn(ID).isEqualTo(literal(UUID.fromString(id)))
				.whereColumn(INSTANCE_ID).isEqualTo(literal(UUID.fromString(getInstanceId())))
				.allowFiltering();
		
		Row resultRow = connection.getSession().execute(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;

		Instant timestamp = resultRow.get(TIMESTAMP, GenericType.INSTANT);
		Boolean compressed = resultRow.get(COMPRESSED, GenericType.BOOLEAN);
		String reportPathStr = resultRow.get(REPORT_PATH, GenericType.STRING);
		ByteBuffer reportByteBuffer = resultRow.get(REPORT_CONTENT, GenericType.BYTE_BUFFER);
		byte[] reportContent = reportByteBuffer == null ? new byte[0] : reportByteBuffer.array();
		String matrixName = resultRow.get(MATRIX_NAME, GenericType.STRING);

		return new StoredReport(UUID.fromString(id), timestamp, reportPathStr, compressed, reportContent, matrixName);
	}

	public String getNextReportId(String reportId)
	{
		return getRecordId(settings.getReportsTableName(), PREV_REPORT_ID, reportId, ID);
	}

	public String getPrevReportId(String reportId)
	{
		return getRecordId(settings.getReportsTableName(), ID, reportId, PREV_REPORT_ID);
	}

	protected void createKeyspace()
	{
		Optional<KeyspaceMetadata> keyspaceExist = connection.getSession().getMetadata().getKeyspace(settings.getKeyspace());
		if(!keyspaceExist.isPresent())
		{
			CreateKeyspace createKs =
					SchemaBuilder.createKeyspace(settings.getKeyspace()).withSimpleStrategy(settings.getKeyspaceReplicationFactor());
			connection.getSession().execute(createKs.build());
		}
	}

	protected void createInstancesTable()
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), INSTANCES_TABLE_DEFAULT_NAME).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withColumn(NAME, DataTypes.TEXT);

		executeQuery(create.asCql());
	}
	
	protected void createStreamsTable()
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), STREAMS_TABLE_DEFAULT_NAME).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withColumn(NAME, DataTypes.TEXT)
				.withColumn(STREAM_DATA, DataTypes.TEXT);
		
		executeQuery(create.asCql());
	}

	protected void createMessagesTable()
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getMessagesTableName()).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withColumn(INSTANCE_ID, DataTypes.UUID)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(TIMESTAMP, DataTypes.TIMESTAMP)
				.withColumn(BATCH_CONTENT, DataTypes.BLOB)
				.withColumn(PREV_BATCH_ID, DataTypes.UUID);
		
		executeQuery(create.asCql());
	}

	protected void createBatchDirMetadataTable()
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getBatchDirMetadataTableName()).ifNotExists()
				.withPartitionKey(BATCH_ID, DataTypes.UUID)
				.withColumn(DIRECTION, DataTypes.TEXT);

		executeQuery(create.asCql());
	}

	protected void createBatchStreamsMetadataTable()
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(),
				settings.getBatchStreamsMetadataTableName()).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withColumn(BATCH_ID, DataTypes.UUID)
				.withColumn(STREAM_ID, DataTypes.UUID);

		executeQuery(create.asCql());
	}

	protected void createReportsTable()
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getReportsTableName()).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withColumn(INSTANCE_ID, DataTypes.UUID)
				.withColumn(TIMESTAMP, DataTypes.TIMESTAMP)
				.withColumn(COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(REPORT_PATH, DataTypes.TEXT)
				.withColumn(REPORT_CONTENT, DataTypes.BLOB)
				.withColumn(PREV_REPORT_ID, DataTypes.UUID)
				.withColumn(MATRIX_NAME, DataTypes.TEXT);

		executeQuery(create.asCql());
	}

	protected void createReportMessagesLinkTable()
	{
		CreateTable create = SchemaBuilder.createTable(settings.getKeyspace(), settings.getReportMsgsLinkTableName()).ifNotExists()
				.withPartitionKey(ID, DataTypes.UUID)
				.withColumn(REPORT_ID, DataTypes.UUID)
				.withColumn(MESSAGES_IDS, DataTypes.listOf(DataTypes.TEXT));

		executeQuery(create.asCql());
	}
	
	protected UUID getInstanceId(String instanceName)
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
			RegularInsert insert = insertInto(settings.getKeyspace(), INSTANCES_TABLE_DEFAULT_NAME)
					.value(ID, literal(id))
					.value(NAME, literal(instanceName));
			executeQuery(insert.asCql());
		}
		
		return id;
	}
	
	protected ResultSet executeQuery(String cqlQuery)
	{
		return connection.getSession().execute(makeSimpleStatement(cqlQuery));
	}


	protected List<StoredMessage> buildMessagesFromRow(Row row) throws IOException
	{
		UUID id = row.get(ID, GenericType.UUID);
		Boolean compressed = row.get(COMPRESSED, GenericType.BOOLEAN);
		
		ByteBuffer contentByteBuffer = row.get(BATCH_CONTENT, GenericType.BYTE_BUFFER);
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
	

	private SimpleStatement makeSimpleStatement(String query)
	{
		return SimpleStatement.newInstance(query).setTimeout(Duration.ofMillis(settings.getTimeout()));
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
			synchronized (writingMonitor)
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
