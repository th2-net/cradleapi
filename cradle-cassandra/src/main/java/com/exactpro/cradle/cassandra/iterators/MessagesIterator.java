/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.cassandra.iterators;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import com.exactpro.cradle.CradleIterator;
import com.exactpro.cradle.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.converters.DetailedMessageBatchConverter;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageFilter;

public class MessagesIterator implements CradleIterator<StoredMessage>
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesIterator.class);
	
	private final MessageBatchIterator batchIterator;
	private final StoredMessageFilter filter;
	private Iterator<StoredMessage> messagesIterator;
	private long returnedMessages;
	private StoredMessage nextMessage;
	private volatile boolean isCanceled = false;
	
	public MessagesIterator(StoredMessageFilter filter, MappedAsyncPagingIterable<DetailedMessageBatchEntity> rows,
			PagingSupplies pagingSupplies, DetailedMessageBatchConverter converter, String queryInfo)
	{
		this.filter = filter;
		this.batchIterator = new MessageBatchIterator(rows, filter == null ? Order.DIRECT : filter.getOrder(),
				pagingSupplies, converter, queryInfo);
	}
	
	
	@Override
	public boolean hasNext()
	{
		if (filter != null && filter.getLimit() > 0 && returnedMessages >= filter.getLimit())
			return false;

		if (messagesIterator == null)
			messagesIterator = getNextMessagesIterator();

		while (messagesIterator != null)
		{
			if ((nextMessage = checkNext()) != null)
				return true;

			messagesIterator = getNextMessagesIterator();
		}

		return false;
	}

	private Iterator<StoredMessage> getNextMessagesIterator()
	{
		logger.trace("Getting messages from next batch");
		if (batchIterator.hasNext())
			return batchIterator.next().iterator();

		return null;
	}
	
	@Override
	public StoredMessage next()
	{
		if (isCanceled)
			return null;
		
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
	
	@Override
	public void cancel()
	{
		isCanceled = true;
		batchIterator.cancel();
	}
	
	private StoredMessage checkNext()
	{
		while (!isCanceled && messagesIterator.hasNext())
		{
			StoredMessage msg = messagesIterator.next();
			if (checkFilter(msg))
				return msg;
		}
		return null;
	}
	
	private boolean checkFilter(StoredMessage message)
	{
		if (filter == null)
			return true;
		
		if (filter.getLeftBoundIndex() > -1 && message.getIndex() < filter.getLeftBoundIndex())
			return false;
		
		if (filter.getIndex() != null && !filter.getIndex().check(message.getIndex()))
			return false;
		if (filter.getTimestampFrom() != null && !filter.getTimestampFrom().check(message.getTimestamp()))
			return false;
		if (filter.getTimestampTo() != null && !filter.getTimestampTo().check(message.getTimestamp()))
			return false;
		return true;
	}
}
