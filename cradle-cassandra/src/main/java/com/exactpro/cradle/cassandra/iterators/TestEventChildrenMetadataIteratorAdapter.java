/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import java.util.Iterator;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.CradleResultSet;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventChildEntity;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventChildConverter;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

public class TestEventChildrenMetadataIteratorAdapter implements CradleResultSet<StoredTestEventMetadata>
{
	private final TestEventChildrenMetadataIterator it;
	
	public TestEventChildrenMetadataIteratorAdapter(MappedAsyncPagingIterable<TestEventChildEntity> rows,
			PagingSupplies pagingSupplies, TestEventChildConverter converter, String queryInfo)
	{
		this.it = new TestEventChildrenMetadataIterator(rows, pagingSupplies, converter, queryInfo);
	}
	
	@Override
	public Iterator<StoredTestEventMetadata> iterator()
	{
		return it;
	}
	
	@Override
	public void cancel()
	{
		it.cancel();
	}
}
