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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public abstract class MessagesLinker<T>
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
	
	
	protected abstract T createLinkedId(String id) throws CradleIdException;
	
	
	protected Collection<T> getManyLinkedsByMessageId(StoredMessageId messageId) throws IOException
	{
		ResultSet rs = getLinkedsByMessageIdResult(messageId);
		Iterator<Row> it = rs.iterator();
		
		Set<T> result = new HashSet<>();
		while (it.hasNext())
		{
			Row r = it.next();
			try
			{
				result.add(createLinkedId(r.getString(linkColumn)));
			}
			catch (CradleIdException e)
			{
				throw new IOException("Could not parse linked ID", e);
			}
		}
		return result;
	}
	
	protected Collection<StoredMessageId> getLinkedMessageIds(T linkedId) throws IOException
	{
		Select selectFrom = prepareQuery(linkedId.toString());
		
		Iterator<Row> resultIterator = exec.executeQuery(selectFrom.asCql(), false).iterator();
		Set<StoredMessageId> ids = new HashSet<>();
		while (resultIterator.hasNext())
		{
			Set<String> currentMessageIds = resultIterator.next().get(MESSAGES_IDS,
					GenericType.setOf(GenericType.STRING));
			if (currentMessageIds != null)
			{
				for (String cid : currentMessageIds)
				{
					try
					{
						StoredMessageId parsedId = StoredMessageId.fromString(cid);
						ids.add(parsedId);
					}
					catch (CradleIdException e)
					{
						throw new IOException("Could not parse message ID", e);
					}
				}
			}
		}
		if (ids.isEmpty())
			return null;
		
		return ids;
	}

	protected boolean isLinkedToMessages(T linkedId) throws IOException
	{
		Select selectFrom = prepareQuery(linkedId.toString())
				.limit(1);
		
		return exec.executeQuery(selectFrom.asCql(), false).one() != null;
	}
	
	
	private Select prepareQuery(String linkedId)
	{
		return selectFrom(keyspace, linksTable)
				.column(MESSAGES_IDS)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(linkColumn).isEqualTo(literal(linkedId));
	}
	private ResultSet getLinkedsByMessageIdResult(StoredMessageId messageId) throws IOException
	{
		Select selectFrom = selectFrom(keyspace, linksTable)
				.column(linkColumn)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(MESSAGES_IDS).contains(literal(messageId.toString()))
				.allowFiltering();  //This is required to use contains()
		return exec.executeQuery(selectFrom.asCql(), false);
	}
}