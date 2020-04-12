/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.linkers;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGES_IDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class MessagesLinker
{
	private final QueryExecutor exec;
	private final String keyspace,
			linksTable,
			linkColumn;
	private final UUID instanceId;
	
	public MessagesLinker(QueryExecutor exec, String keyspace, String linksTable, String linkColumn, UUID instanceId)
	{
		this.exec = exec;
		this.keyspace = keyspace;
		this.linksTable = linksTable;
		this.linkColumn = linkColumn;
		this.instanceId = instanceId;
	}
	
	protected String getLinkedByMessageId(StoredMessageId messageId) throws IOException
	{
		Select selectFrom = selectFrom(keyspace, linksTable)
				.column(linkColumn)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(MESSAGES_IDS).contains(literal(messageId.toString()))
				.allowFiltering();  //This is required to use contains()
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		String id = resultRow.getString(linkColumn);
		if (id == null)
			return null;
		
		return id;
	}

	protected List<StoredMessageId> getLinkedMessageIds(String linkedId) throws IOException
	{
		Select selectFrom = selectFrom(keyspace, linksTable)
				.column(MESSAGES_IDS)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(linkColumn).isEqualTo(literal(linkedId));
		
		Iterator<Row> resultIterator = exec.executeQuery(selectFrom.asCql()).iterator();
		List<StoredMessageId> ids = new ArrayList<>();
		while (resultIterator.hasNext())
		{
			List<String> currentMessageIds = resultIterator.next().get(MESSAGES_IDS,
					GenericType.listOf(GenericType.STRING));
			if (currentMessageIds != null)
			{
				for (String cid : currentMessageIds)
				{
					StoredMessageId parsedId = parseMessageId(cid);
					ids.add(parsedId);
				}
			}
		}
		if (ids.isEmpty())
			return null;
		
		return ids;
	}

	protected boolean isLinkedToMessages(String linkedId) throws IOException
	{
		Select selectFrom = selectFrom(keyspace, linksTable)
				.column(MESSAGES_IDS)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(linkColumn).isEqualTo(literal(linkedId))
				.limit(1);
		
		return exec.executeQuery(selectFrom.asCql()).one() != null;
	}
	
	private StoredMessageId parseMessageId(String id) throws IOException
	{
		String[] parts = id.split(StoredMessageId.IDS_DELIMITER);
		if (parts.length < 2)
			throw new IOException("Message ID ("+id+") should contain batch ID and message index delimited with '"+StoredMessageId.IDS_DELIMITER+"'");
		
		int index;
		try
		{
			index = Integer.parseInt(parts[1]);
		}
		catch (NumberFormatException e)
		{
			throw new IOException("Invalid message index ("+parts[1]+") in message ID '"+id+"'");
		}
		
		return new StoredMessageId(new StoredMessageBatchId(parts[0]), index);
	}
}