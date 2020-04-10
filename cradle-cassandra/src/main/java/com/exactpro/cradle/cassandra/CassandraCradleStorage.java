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
import com.exactpro.cradle.ReportsMessagesLinker;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.StoredTestEvent;
import com.exactpro.cradle.StreamsMessagesLinker;
import com.exactpro.cradle.TestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.iterators.MessagesIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.ReportsIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.TestEventsIteratorAdapter;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.ReportUtils;
import com.exactpro.cradle.cassandra.utils.TestEventUtils;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

import org.apache.commons.lang3.StringUtils;
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
	private ReportsMessagesLinker reportsMessagesLinker;
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
	public String storeReport(StoredReport report) throws IOException
	{
		byte[] reportContent = report.getContent();
		boolean toCompress = isNeedCompressReport(reportContent);
		if (toCompress)
		{
			try
			{
				reportContent = CompressionUtils.compressData(reportContent);
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
				content = CompressionUtils.compressData(content);
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
				content = CompressionUtils.compressData(content);
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
				content = CompressionUtils.compressData(content);
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
		Select selectFrom = CassandraMessageUtils.prepareSelect(settings.getKeyspace(), settings.getMessagesTableName(), instanceUuid)
				.whereColumn(ID).isEqualTo(literal(id.getBatchId().toString()));
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		return CassandraMessageUtils.toMessage(resultRow, id.getIndex());
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
		return new MessagesIteratorAdapter(exec, settings.getKeyspace(), settings.getMessagesTableName(), instanceUuid);
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
	
	
	private void storeMessageBatchData(StoredMessageBatch batch) throws IOException
	{
		byte[] batchContent = MessageUtils.serializeMessages(batch.getMessages());
		
		Instant batchStartTimestamp = batch.getTimestamp();
		
		boolean toCompress = isNeedToCompressBatchContent(batchContent);
		if (toCompress)
		{
			try
			{
				batchContent = CompressionUtils.compressData(batchContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress batch contents (ID: '%s') to save in storage",
						batch.getId()), e);
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
				.ifNotExists();
		exec.executeQuery(insert.asCql());
	}

	private void storeMessageBatchMetadata(StoredMessageBatch batch) throws IOException
	{
		for (Entry<String, Set<StoredMessageId>> streamMessages : batch.getStreamsMessages().entrySet())
			linkMessagesTo(streamMessages.getValue(), settings.getStreamMsgsLinkTableName(), STREAM_NAME, streamMessages.getKey());
	}
	
	
	private boolean isNeedToCompressBatchContent(byte[] batchContentBytes)
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
}