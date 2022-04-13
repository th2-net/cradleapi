/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.StoredMessageBatch;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class FilteredGroupedMessageBatchIterator extends MappedIterator<StoredMessageBatch, StoredMessageBatch>
{
	private FilterForGreater<Instant> filterFrom;
	private FilterForLess<Instant> filterTo;


	public FilteredGroupedMessageBatchIterator(Iterator<StoredMessageBatch> sourceIterator, GroupedMessageFilter filter,
			int limit, AtomicInteger returned)
	{
		super(sourceIterator, limit, returned);
		filterFrom = filter.getFrom();
		filterTo = filter.getTo(); 
	}

	@Override
	Iterator<StoredMessageBatch> createTargetIterator(Iterator<StoredMessageBatch> sourceIterator)
	{
		Predicate<StoredMessageBatch> filterPredicate = createFilterPredicate();
		return Streams.stream(sourceIterator)
				.filter(filterPredicate)
				.iterator();
	}

	private Predicate<StoredMessageBatch> createFilterPredicate()
	{
		return storedMessageBatch ->
				(filterFrom == null || filterFrom.check(storedMessageBatch.getLastTimestamp()))
						&& (filterTo == null || filterTo.check(storedMessageBatch.getFirstTimestamp()));
	}
}
