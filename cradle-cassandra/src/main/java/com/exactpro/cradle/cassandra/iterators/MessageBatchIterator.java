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
import java.util.Collection;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.converters.DetailedMessageBatchConverter;
import com.exactpro.cradle.cassandra.retries.RetrySupplies;
import com.exactpro.cradle.messages.StoredMessage;

public class MessageBatchIterator extends ConvertingPagedIterator<Collection<StoredMessage>, DetailedMessageBatchEntity>
{
	private final Order order;
	
	public MessageBatchIterator(MappedAsyncPagingIterable<DetailedMessageBatchEntity> rows, Order order, 
			RetrySupplies retrySupplies, DetailedMessageBatchConverter converter)
	{
		super(rows, retrySupplies, converter);
		this.order = order;
	}
	
	@Override
	protected Collection<StoredMessage> convertEntity(DetailedMessageBatchEntity entity) throws IOException
	{
		return entity.toStoredMessages(order);
	}
}
