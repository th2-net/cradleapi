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
import com.exactpro.cradle.daomodule.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.messages.StoredMessageBatch;

import java.util.Iterator;

public class StoredMessageBatchAdapter implements Iterable<StoredMessageBatch>
{
	private final MappedAsyncPagingIterable<DetailedMessageBatchEntity> entities;
	private final CradleObjectsFactory objectsFactory;
	private int limit;
	
	public StoredMessageBatchAdapter(MappedAsyncPagingIterable<DetailedMessageBatchEntity> entities, 
			CradleObjectsFactory objectsFactory, int limit)
	{
		this.entities = entities;
		this.objectsFactory = objectsFactory;
		this.limit = limit;
	}

	@Override
	public Iterator<StoredMessageBatch> iterator()
	{
		return new StoredMessageBatchIterator(entities, objectsFactory, limit);
	}
}
