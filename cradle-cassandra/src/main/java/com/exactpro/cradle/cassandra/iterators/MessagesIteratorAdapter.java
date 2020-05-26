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

import com.datastax.oss.driver.api.core.PagingIterable;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageFilter;

/**
 * Wrapper for {@link PagingIterable}. 
 * Converts {@link MessageBatchEntity} into {@link StoredMessage} while iterating.
 * Also applies given filter to exclude unnecessary results for iterator.
 */
public class MessagesIteratorAdapter implements Iterable<StoredMessage>
{
	private final StoredMessageFilter filter;
	private final PagingIterable<MessageBatchEntity> entities;
	
	public MessagesIteratorAdapter(StoredMessageFilter filter, PagingIterable<MessageBatchEntity> entities) throws IOException
	{
		this.filter = filter;
		this.entities = entities;
	}
	
	@Override
	public Iterator<StoredMessage> iterator()
	{
		return new MessagesIterator(entities.iterator(), filter);
	}
}