/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.iterators;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageFilter;

public class MessagesIterator implements Iterator<StoredMessage>
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesIterator.class);
	
	private final Iterator<MessageBatchEntity> entitiesIterator;
	private final StoredMessageFilter filter;
	private Iterator<StoredMessage> batchIterator;
	private long returnedMessages;
	private StoredMessage nextMessage;
	
	public MessagesIterator(Iterator<MessageBatchEntity> entitiesIterator, StoredMessageFilter filter)
	{
		this.entitiesIterator = entitiesIterator;
		this.filter = filter;
	}
	
	
	@Override
	public boolean hasNext()
	{
		if (filter.getLimit() > 0 && returnedMessages >= filter.getLimit())
			return false;
		
		if (batchIterator != null)
		{
			if ((nextMessage = checkNext()) != null)
				return true;
			batchIterator = null;
		}
		
		if (!entitiesIterator.hasNext())
			return false;
		
		try
		{
			logger.trace("Getting messages from next batch");
			batchIterator = entitiesIterator.next().toStoredMessages().iterator();
		}
		catch (IOException e)
		{
			throw new RuntimeException("Error while getting messages from next batch", e);
		}
		return hasNext();
	}
	
	@Override
	public StoredMessage next()
	{
		if (nextMessage == null)  //Maybe, hasNext() wasn't called
		{
			if (!hasNext())
				return null;
		}
		
		StoredMessage result = nextMessage;
		nextMessage = null;
		returnedMessages++;
		return result;
	}
	
	
	private StoredMessage checkNext()
	{
		while (batchIterator.hasNext())
		{
			StoredMessage msg = batchIterator.next();
			if (checkFilter(msg))
				return msg;
		}
		return null;
	}
	
	private boolean checkFilter(StoredMessage message)
	{
		if (filter.getIndex() != null && !filter.getIndex().check(message.getIndex()))
			return false;
		if (filter.getTimestampFrom() != null && !filter.getTimestampFrom().check(message.getTimestamp()))
			return false;
		if (filter.getTimestampTo() != null && !filter.getTimestampTo().check(message.getTimestamp()))
			return false;
		return true;
	}
}