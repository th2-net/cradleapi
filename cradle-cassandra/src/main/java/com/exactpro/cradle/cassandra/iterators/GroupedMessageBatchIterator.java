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
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

public class GroupedMessageBatchIterator implements Iterator<StoredMessageBatch>
{
	private final Instant filterFrom, filterTo;
	private final PagedIterator<GroupedMessageBatchEntity> it;
	private StoredMessageBatch nextCandidate;
	
	public GroupedMessageBatchIterator(
			MappedAsyncPagingIterable<GroupedMessageBatchEntity> rows,
			PagingSupplies pagingSupplies,
			EntityConverter<GroupedMessageBatchEntity> converter,
			String queryInfo, Instant filterFrom, Instant filterTo)
	{
		this.it = new PagedIterator<>(rows, pagingSupplies, converter, queryInfo);
		this.filterFrom = filterFrom;
		this.filterTo = filterTo;
	}

	@Override
	public boolean hasNext()
	{
		if (nextCandidate != null)
			return true;
		
		while (nextCandidate == null && it.hasNext())
		{
			GroupedMessageBatchEntity entity = it.next();
			if (checkBoundaries(entity))
			{
				try
				{
					nextCandidate = convertEntity(entity);
				}
				catch (IOException e)
				{
					throw new RuntimeException("Error while getting next data row", e);
				}
				return true;
			}
		}
		
		return false;
	}

	private boolean checkBoundaries(GroupedMessageBatchEntity entity)
	{
		Instant left = entity.getFirstMessageTimestamp();
		Instant right = entity.getLastMessageTimestamp();
		
		return left.compareTo(filterTo) <= 0 && right.compareTo(filterFrom) >= 0;
	}

	@Override
	public StoredMessageBatch next()
	{
		StoredMessageBatch result = nextCandidate;
		nextCandidate = null;
		return result;
	}

	protected StoredMessageBatch convertEntity(GroupedMessageBatchEntity entity) throws IOException
	{
		try
		{
			return entity.getBatchEntity().toStoredMessageBatch();
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Error occurred while converting entity to StoredMessageBatch",e);
		}
	}
}
