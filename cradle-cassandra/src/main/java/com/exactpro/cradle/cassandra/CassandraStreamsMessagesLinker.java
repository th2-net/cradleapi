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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.StreamsMessagesLinker;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CassandraStreamsMessagesLinker extends MessagesLinker implements StreamsMessagesLinker
{
	private final CradleStorage storage;
	
	public CassandraStreamsMessagesLinker(QueryExecutor exec, String keyspace, String linksTable, String linkColumn,
			UUID instanceId, CradleStorage storage)
	{
		super(exec, keyspace, linksTable, linkColumn, instanceId);
		this.storage = storage;
	}
	
	@Override
	public List<StoredMessageId> getMessageIdsOfStream(String streamName) throws IOException
	{
		return getMessageIdsByLinkedId(storage.getStreamId(streamName));
	}
	
	@Override
	public boolean isStreamLinkedToMessages(String streamName) throws IOException
	{
		return isLinkedToMessages(storage.getStreamId(streamName));
	}
}