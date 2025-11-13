/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class FilteredGroupedMessageBatchIterator extends MappedIterator<StoredGroupedMessageBatch, StoredGroupedMessageBatch>
{
	public FilteredGroupedMessageBatchIterator(Iterator<StoredGroupedMessageBatch> sourceIterator, GroupedMessageFilter filter,
			int limit, AtomicInteger returned)
	{
		super(createTargetIterator(sourceIterator, filter), limit, returned);
	}


	private static Iterator<StoredGroupedMessageBatch> createTargetIterator(Iterator<StoredGroupedMessageBatch> sourceIterator, GroupedMessageFilter filter)
	{
		Predicate<StoredGroupedMessageBatch> filterPredicate = createFilterPredicate(filter);
		return Iterators.filter(sourceIterator, filterPredicate);
	}

	private static Predicate<StoredGroupedMessageBatch> createFilterPredicate(GroupedMessageFilter filter)
	{
		return storedMessageBatch ->
				(filter.getFrom() == null || filter.getFrom().check(storedMessageBatch.getLastTimestamp()))
						&& (filter.getTo() == null || filter.getTo().check(storedMessageBatch.getFirstTimestamp()));
	}
}
