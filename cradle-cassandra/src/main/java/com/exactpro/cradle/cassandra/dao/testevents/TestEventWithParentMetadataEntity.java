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

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.exactpro.cradle.cassandra.StorageConstants.END_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.END_TIME;
import static com.exactpro.cradle.cassandra.StorageConstants.EVENT_BATCH;
import static com.exactpro.cradle.cassandra.StorageConstants.EVENT_COUNT;
import static com.exactpro.cradle.cassandra.StorageConstants.NAME;
import static com.exactpro.cradle.cassandra.StorageConstants.SUCCESS;
import static com.exactpro.cradle.cassandra.StorageConstants.TYPE;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventMetadata;

/**
 * Contains metadata of test event with parent to extend with partition and clustering fields
 */
public abstract class TestEventWithParentMetadataEntity extends TestEventMetadataEntity
{
	public TestEventWithParentMetadataEntity()
	{
	}
	
	public TestEventWithParentMetadataEntity(StoredTestEvent event, UUID instanceId)
	{
		super(event, instanceId);
		
		StoredTestEventId parentId = event.getParentId();
		this.setRoot(parentId == null);
		this.setParentId(parentId != null ? parentId.toString() : null);
	}
	
	
	public abstract boolean isRoot();
	public abstract void setRoot(boolean root);
	
	public abstract String getParentId();
	public abstract void setParentId(String parentId);
	
	
	public StoredTestEventMetadata toStoredTestEventMetadata()
	{
		StoredTestEventMetadata result =  super.toStoredTestEventMetadata();
		String parentId = getParentId();
		if (parentId != null)
			result.setParentId(new StoredTestEventId(parentId));
		return result;
	}
}