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

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventEntity;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

public class RootTestEventsMetadataIterator implements Iterator<StoredTestEventMetadata>
{
	private final Logger logger = LoggerFactory.getLogger(RootTestEventsMetadataIterator.class);
	
	private final Iterator<RootTestEventEntity> rows;
	
	public RootTestEventsMetadataIterator(Iterator<RootTestEventEntity> rows)
	{
		this.rows = rows;
	}
	
	@Override
	public boolean hasNext()
	{
		return rows.hasNext();
	}
	
	@Override
	public StoredTestEventMetadata next()
	{
		logger.trace("Getting next test event metadata");
		
		RootTestEventEntity r = rows.next();
		return r.toStoredTestEventMetadata();
	}
}
