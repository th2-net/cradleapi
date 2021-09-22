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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;

import java.time.LocalDate;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Entity
public class ChildrenDatesEventEntity
{
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;

	@PartitionKey(1)
	@CqlName(PARENT_ID)
	private String parentId = ROOT_EVENT_PARENT_ID;

	@ClusteringColumn(0)
	@CqlName(START_DATE)
	private LocalDate startDate;

	public ChildrenDatesEventEntity()
	{
	}

	public ChildrenDatesEventEntity(StoredTestEvent event, UUID instanceId)
	{
		setInstanceId(instanceId);
		setParentId(event.getParentId() == null ? ROOT_EVENT_PARENT_ID : event.getParentId().toString());
		setStartDate(event.getStartTimestamp().atZone(CassandraCradleStorage.TIMEZONE_OFFSET).toLocalDate());
	}

	public UUID getInstanceId()
	{
		return instanceId;
	}

	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
	}

	public String getParentId()
	{
		return parentId;
	}

	public void setParentId(String parentId)
	{
		this.parentId = parentId;
	}

	public LocalDate getStartDate()
	{
		return startDate;
	}

	public void setStartDate(LocalDate startDate)
	{
		this.startDate = startDate;
	}
}
