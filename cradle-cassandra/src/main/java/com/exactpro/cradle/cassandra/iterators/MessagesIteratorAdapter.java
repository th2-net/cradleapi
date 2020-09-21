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

import java.io.IOException;
import java.util.Iterator;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
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
	private final PagingIterable<DetailedMessageBatchEntity> entities;
	
	public MessagesIteratorAdapter(StoredMessageFilter filter, PagingIterable<DetailedMessageBatchEntity> entities) throws IOException
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
