/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.exceptions.CradleStorageException;
import com.exactpro.cradle.messages.StoredMessageBatch;

import java.io.IOException;

public class StoredMessageBatchIterator extends ConvertingPagedIterator<StoredMessageBatch, DetailedMessageBatchEntity>
{
	private final CradleObjectsFactory objectsFactory;
	private final int limit;
	private long returnedEntities;
	
	public StoredMessageBatchIterator(MappedAsyncPagingIterable<DetailedMessageBatchEntity> rows,
			CradleObjectsFactory objectsFactory, int limit)
	{
		super(rows);
		this.objectsFactory = objectsFactory;
		this.limit = limit;
	}

	@Override
	public boolean hasNext()
	{
		if (limit > 0 && returnedEntities >= limit)
			return false;

		return super.hasNext();
	}

	@Override
	public StoredMessageBatch next()
	{
		returnedEntities++;
		return super.next();
	}

	@Override
	protected StoredMessageBatch convertEntity(DetailedMessageBatchEntity entity) throws IOException
	{
		try
		{
			return entity.toStoredMessageBatch(objectsFactory.createMessageBatch());
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Error occurred while converting entity to StoredMessageBatch",e);
		}
	}
	
}
