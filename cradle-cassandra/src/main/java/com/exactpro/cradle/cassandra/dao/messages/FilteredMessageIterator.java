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
import com.exactpro.cradle.Order;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.MessageFilter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class FilteredMessageIterator extends MappedIterator<StoredMessageBatch, StoredMessage>
{
	public FilteredMessageIterator(Iterator<StoredMessageBatch> batchIterator, MessageFilter filter, int limit,
			AtomicInteger returned)
	{
		super(createTargetIterator(batchIterator, filter), limit, returned);
	}

	private static Iterator<StoredMessage> createTargetIterator(Iterator<StoredMessageBatch> sourceIterator, MessageFilter filter)
	{
		Predicate<StoredMessage> filterPredicate = createFilterPredicate(filter);
		return Streams.stream(sourceIterator)
				.flatMap(b -> {
					if (filter.getOrder().equals(Order.REVERSE)) {
						var elements  = new ArrayList<>(b.getMessages());
						Collections.reverse(elements);

						return elements.stream();
					}

					return b.getMessages().stream();
				})
				.filter(filterPredicate)
				.iterator();
	}

	private static Predicate<StoredMessage> createFilterPredicate(MessageFilter filter)
	{
		FilterForAny<Long> sequence = filter == null ? null : filter.getSequence();
		FilterForGreater<Instant> timestampFrom = filter == null ? null : filter.getTimestampFrom();
		FilterForLess<Instant> timestampTo = filter == null ? null : filter.getTimestampTo();
		return storedMessage ->
					(sequence == null || sequence.check(storedMessage.getId().getSequence()))
					&& (timestampFrom == null || timestampFrom.check(storedMessage.getTimestamp()))
					&& (timestampTo == null || timestampTo.check(storedMessage.getTimestamp()));
	}
}
