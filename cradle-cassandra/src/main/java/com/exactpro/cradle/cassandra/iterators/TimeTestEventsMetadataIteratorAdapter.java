/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import java.io.IOException;
import java.util.Iterator;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.exactpro.cradle.cassandra.dao.testevents.TimeTestEventEntity;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

public class TimeTestEventsMetadataIteratorAdapter implements Iterable<StoredTestEventMetadata>
{
	private final MappedAsyncPagingIterable<TimeTestEventEntity> rows;
	
	public TimeTestEventsMetadataIteratorAdapter(MappedAsyncPagingIterable<TimeTestEventEntity> rows)
	{
		this.rows = rows;
	}
	
	@Override
	public Iterator<StoredTestEventMetadata> iterator()
	{
		return new TimeTestEventsMetadataIterator(rows);
	}
}
