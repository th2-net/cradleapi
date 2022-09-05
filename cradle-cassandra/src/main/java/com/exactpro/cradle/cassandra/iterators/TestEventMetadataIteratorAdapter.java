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

package com.exactpro.cradle.cassandra.iterators;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMetadataEntity;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventMetadataConverter;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

import java.time.Instant;
import java.util.Iterator;

public class TestEventMetadataIteratorAdapter implements Iterable<StoredTestEventMetadata>
{
	private final MappedAsyncPagingIterable<TestEventMetadataEntity> rows;
	private final PagingSupplies pagingSupplies;
	private final TestEventMetadataConverter converter;
	private final String queryInfo;
	private final Instant actualFrom;

	public TestEventMetadataIteratorAdapter(
			MappedAsyncPagingIterable<TestEventMetadataEntity> rows,
			PagingSupplies pagingSupplies,
			TestEventMetadataConverter converter,
			Instant actualFrom,
			String queryInfo)
	{
		this.rows = rows;
		this.pagingSupplies = pagingSupplies;
		this.converter = converter;
		this.actualFrom = actualFrom;
		this.queryInfo = queryInfo;
	}

	@Override
	public Iterator<StoredTestEventMetadata> iterator()
	{
		return new TestEventsMetadataIterator(rows, pagingSupplies, converter, actualFrom, queryInfo);
	}
}
