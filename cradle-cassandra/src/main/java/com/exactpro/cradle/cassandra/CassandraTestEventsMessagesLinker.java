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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.TestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CassandraTestEventsMessagesLinker extends MessagesLinker implements TestEventsMessagesLinker
{
	public CassandraTestEventsMessagesLinker(QueryExecutor exec, String keyspace, String linksTable, String linkColumn,
			UUID instanceId)
	{
		super(exec, keyspace, linksTable, linkColumn, instanceId);
	}

	@Override
	public String getTestEventIdByMessageId(StoredMessageId messageId) throws IOException
	{
		return getLinkedByMessageId(messageId);
	}

	@Override
	public List<StoredMessageId> getMessageIdsByEventId(String eventId) throws IOException
	{
		return getLinkedMessageIds(eventId);
	}

	@Override
	public boolean isTestEventLinkedToMessages(String eventId) throws IOException
	{
		return isLinkedToMessages(eventId);
	}
}