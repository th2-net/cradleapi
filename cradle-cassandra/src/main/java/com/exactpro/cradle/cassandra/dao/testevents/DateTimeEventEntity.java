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

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import static com.exactpro.cradle.cassandra.StorageConstants.*;
import static com.exactpro.cradle.cassandra.StorageConstants.START_TIME;

@Entity
public class DateTimeEventEntity extends DateEventEntity
{
	@PartitionKey(0)
	@CqlName(INSTANCE_ID)
	private UUID instanceId;

	@PartitionKey(1)
	@CqlName(ID)
	private String id;

	@CqlName(START_TIME)
	private LocalTime startTime;

	public DateTimeEventEntity()
	{
	}
	
	public DateTimeEventEntity(StoredTestEvent event, UUID instanceId)
	{
		setInstanceId(instanceId);
		setId(event.getId().toString());
		setStartTimestamp(event.getStartTimestamp());
	}

	public UUID getInstanceId()
	{
		return instanceId;
	}

	public void setInstanceId(UUID instanceId)
	{
		this.instanceId = instanceId;
	}

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public LocalTime getStartTime()
	{
		return startTime;
	}

	public void setStartTime(LocalTime startTime)
	{
		this.startTime = startTime;
	}


	@Transient
	public Instant getStartTimestamp()
	{
		LocalDate sd = getStartDate();
		LocalTime st = getStartTime();
		if (sd == null || st == null)
			return null;
		return LocalDateTime.of(sd, st).toInstant(CassandraCradleStorage.TIMEZONE_OFFSET);
	}

	@Transient
	public void setStartTimestamp(Instant timestamp)
	{
		if (timestamp == null)
		{
			setStartDate(null);
			setStartTime(null);
			return;
		}
		LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET);
		setStartDate(ldt.toLocalDate());
		setStartTime(ldt.toLocalTime());
	}

}
