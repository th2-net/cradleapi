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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CassandraTestEventsMessagesLinker extends MessagesLinker<StoredTestEventId> implements TestEventsMessagesLinker
{
	public CassandraTestEventsMessagesLinker(QueryExecutor exec, String keyspace, String linksTable, String linkColumn,
			UUID instanceId)
	{
		super(exec, keyspace, linksTable, linkColumn, instanceId);
	}

	@Override
	public List<StoredTestEventId> getTestEventIdsByMessageId(StoredMessageId messageId) throws IOException
	{
		return getManyLinkedsByMessageId(messageId);
	}

	@Override
	public List<StoredMessageId> getMessageIdsByTestEventId(StoredTestEventId eventId) throws IOException
	{
		return getLinkedMessageIds(eventId);
	}

	@Override
	public boolean isTestEventLinkedToMessages(StoredTestEventId eventId) throws IOException
	{
		return isLinkedToMessages(eventId);
	}
	
	
	@Override
	protected StoredTestEventId createLinkedId(String id) throws CradleIdException
	{
		return StoredTestEventId.fromString(id);
	}
}