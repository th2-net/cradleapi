/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class FilteredMessageIterator implements Iterator<StoredMessage>
{
	private final int limit;
	private final AtomicInteger returned;
	private final Iterator<StoredMessage> it;

	public FilteredMessageIterator(Iterator<StoredMessageBatch> batchIterator, StoredMessageFilter filter, int limit,
			AtomicInteger returned)
	{
		this.limit = limit;
		this.returned = returned;
		Predicate<StoredMessage> filterPredicate = createFilterPredicate(filter);
		this.it = Streams.stream(batchIterator)
				.flatMap(b -> b.getMessages().stream())
				.filter(filterPredicate)
				.iterator();
	}

	private Predicate<StoredMessage> createFilterPredicate(StoredMessageFilter filter)
	{
		if (filter == null)
			return storedMessage -> true;

		FilterForAny<StoredMessageId> messageId = filter.getMessageId();
		FilterForGreater<Instant> timestampFrom = filter.getTimestampFrom();
		FilterForLess<Instant> timestampTo = filter.getTimestampTo();

		return storedMessage -> messageId == null || (messageId.check(storedMessage.getId())
				&& timestampFrom == null || (timestampFrom.check(storedMessage.getTimestamp())
				&& timestampTo == null || timestampTo.check(storedMessage.getTimestamp())));
	}

	@Override
	public boolean hasNext()
	{
		return (limit <= 0 || returned.get() < limit) && it.hasNext();
	}

	@Override
	public StoredMessage next()
	{
//		if (limit > 0 && returned.get() >= limit)
//			return null;

		returned.incrementAndGet();
		return it.next();
	}
}
