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

import static com.exactpro.cradle.cassandra.StorageConstants.ID;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.PARENT_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.ROOT;
import static com.exactpro.cradle.cassandra.StorageConstants.START_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.START_TIME;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.testevents.StoredTestEvent;

/**
 * Contains test event metadata related to given parent
 */
@Entity
public class TestEventChildEntity extends TestEventWithParentMetadataEntity
{
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;
	
	@PartitionKey(1)
	@CqlName(PARENT_ID)
	private String parentId;
	
	@PartitionKey(2)
	@CqlName(START_DATE)
	private LocalDate startDate;
	
	@ClusteringColumn(0)
	@CqlName(START_TIME)
	private LocalTime startTime;
	
	@ClusteringColumn(1)
	@CqlName(ID)
	private String id;
	
	@CqlName(ROOT)
	private boolean root;
	
	
	public TestEventChildEntity()
	{
	}
	
	public TestEventChildEntity(StoredTestEvent event, UUID instanceId)
	{
		super(event, instanceId);
	}
	
	
	@Override
	public UUID getInstanceId()
	{
		return instanceId;
	}
	
	@Override
	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
	}
	
	
	@Override
	public String getId()
	{
		return id;
	}
	
	@Override
	public void setId(String id)
	{
		this.id = id;
	}
	
	
	@Override
	public boolean isRoot()
	{
		return root;
	}
	
	@Override
	public void setRoot(boolean root)
	{
		this.root = root;
	}
	
	
	@Override
	public String getParentId()
	{
		return parentId;
	}
	
	@Override
	public void setParentId(String parentId)
	{
		this.parentId = parentId;
	}
	
	
	@Override
	public LocalDate getStartDate()
	{
		return startDate;
	}
	
	@Override
	public void setStartDate(LocalDate startDate)
	{
		this.startDate = startDate;
	}
	
	@Override
	public LocalTime getStartTime()
	{
		return startTime;
	}
	
	@Override
	public void setStartTime(LocalTime startTime)
	{
		this.startTime = startTime;
	}
}