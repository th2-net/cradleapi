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

package com.exactpro.cradle.cassandra.dao;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Parent for main Cradle entities stored in Cassandra
 */
public abstract class CradleEntity
{
	@CqlName(STORED_DATE)
	private LocalDate storedDate;
	@CqlName(STORED_TIME)
	private LocalTime storedTime;
	@CqlName(COMPRESSED)
	private boolean compressed;
	@CqlName(LABELS)
	private Set<String> labels;
	@CqlName(CONTENT)
	private ByteBuffer content;
	
	public LocalDate getStoredDate()
	{
		return storedDate;
	}
	
	public void setStoredDate(LocalDate storedDate)
	{
		this.storedDate = storedDate;
	}
	
	public LocalTime getStoredTime()
	{
		return storedTime;
	}
	
	public void setStoredTime(LocalTime storedTime)
	{
		this.storedTime = storedTime;
	}
	
	@Transient
	public Instant getStoredTimestamp()
	{
		return TimeUtils.toInstant(getStoredDate(), getStoredTime());
	}
	
	@Transient
	public void setStoredTimestamp(Instant timestamp)
	{
		if (timestamp != null)
		{
			LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
			setStoredDate(ldt.toLocalDate());
			setStoredTime(ldt.toLocalTime());
		}
		else
		{
			setStoredDate(null);
			setStoredTime(null);
		}
	}
	
	
	public boolean isCompressed()
	{
		return compressed;
	}
	
	public void setCompressed(boolean compressed)
	{
		this.compressed = compressed;
	}
	
	
	public Set<String> getLabels()
	{
		return labels;
	}
	
	public void setLabels(Set<String> labels)
	{
		this.labels = labels;
	}
	
	
	public ByteBuffer getContent()
	{
		return content;
	}
	
	public void setContent(ByteBuffer content)
	{
		this.content = content;
	}
}