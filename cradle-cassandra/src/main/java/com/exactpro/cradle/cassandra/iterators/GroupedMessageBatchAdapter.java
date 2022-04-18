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

package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.EntityConverter;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.converters.GroupedMessageBatchConverter;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.messages.StoredMessageBatch;

import java.time.Instant;
import java.util.Iterator;

public class GroupedMessageBatchAdapter implements Iterable<StoredMessageBatch>
{
	private final MappedAsyncPagingIterable<GroupedMessageBatchEntity> entities;
	private final PagingSupplies pagingSupplies;
	private final GroupedMessageBatchConverter converter;
	private final String queryInfo;
	private final Instant filterFrom, filterTo;
	
	public GroupedMessageBatchAdapter(MappedAsyncPagingIterable<GroupedMessageBatchEntity> entities,
			PagingSupplies pagingSupplies, GroupedMessageBatchConverter converter,
			String queryInfo, Instant filterFrom, Instant filterTo)
	{
		this.entities = entities;
		this.pagingSupplies = pagingSupplies;
		this.converter = converter;
		this.queryInfo = queryInfo;
		this.filterFrom = filterFrom;
		this.filterTo = filterTo;
	}

	@Override
	public Iterator<StoredMessageBatch> iterator()
	{
		return new GroupedMessageBatchIterator(entities, pagingSupplies, converter, queryInfo, filterFrom, filterTo);
	}
}
