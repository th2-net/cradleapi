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
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventConverter;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;

import java.util.Iterator;

public class TestEventDataIteratorAdapter implements Iterable<StoredTestEventWrapper>
{
	private final MappedAsyncPagingIterable<TestEventEntity> rows;
	private final PagingSupplies pagingSupplies;
	private final TestEventConverter converter;
	private final String queryInfo;
	private final CradleObjectsFactory objectsFactory;

	public TestEventDataIteratorAdapter(MappedAsyncPagingIterable<TestEventEntity> rows, CradleObjectsFactory objectsFactory,
			PagingSupplies pagingSupplies, TestEventConverter converter, String queryInfo)
	{
		this.rows = rows;
		this.pagingSupplies = pagingSupplies;
		this.converter = converter;
		this.queryInfo = queryInfo;
		this.objectsFactory = objectsFactory;
	}

	@Override
	public Iterator<StoredTestEventWrapper> iterator()
	{
		return new TestEventsDataIterator(rows, pagingSupplies, converter, objectsFactory, queryInfo);
	}
}
