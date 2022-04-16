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

package com.exactpro.cradle.cassandra.dao.books;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.utils.TimeUtils;

@Entity
public class PageEntity
{
	public static final String FIELD_PART = "part",
			FIELD_START_DATE = "start_date",
			FIELD_START_TIME = "start_time",
			FIELD_NAME = "name",
			FIELD_COMMENT = "comment",
			FIELD_END_DATE = "end_date",
			FIELD_END_TIME = "end_time",
			FIELD_REMOVED = "removed";
	@PartitionKey(0)
	@CqlName(FIELD_PART)
	private String part;
	
	@ClusteringColumn(0)
	@CqlName(FIELD_START_DATE)
	private LocalDate startDate;
	
	@ClusteringColumn(1)
	@CqlName(FIELD_START_TIME)
	private LocalTime startTime;
	
	@CqlName(FIELD_NAME)
	private String name;
	
	@CqlName(FIELD_COMMENT)
	private String comment;
	
	@CqlName(FIELD_END_DATE)
	private LocalDate endDate;
	
	@CqlName(FIELD_END_TIME)
	private LocalTime endTime;
	
	@CqlName(FIELD_REMOVED)
	private Instant removed;
	
	
	public PageEntity()
	{
	}
	
	public PageEntity(String part, String name, Instant started, String comment, Instant ended)
	{
		LocalDateTime startedLdt = TimeUtils.toLocalTimestamp(started);
		
		this.part = part;
		this.name = name;
		this.startDate = startedLdt.toLocalDate();
		this.startTime = startedLdt.toLocalTime();
		this.comment = comment;
		
		if (ended != null)
		{
			LocalDateTime endedLdt = TimeUtils.toLocalTimestamp(ended);
			this.endDate = endedLdt.toLocalDate();
			this.endTime = endedLdt.toLocalTime();
		}
	}
	
	public PageEntity(PageInfo pageInfo)
	{
		this(pageInfo.getId().getBookId().getName(), pageInfo.getId().getName(), pageInfo.getStarted(), pageInfo.getComment(), pageInfo.getEnded());
	}
	
	
	public String getPart()
	{
		return part;
	}
	
	public void setPart(String part)
	{
		this.part = part;
	}
	
	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	
	public LocalDate getStartDate()
	{
		return startDate;
	}
	
	public void setStartDate(LocalDate startDate)
	{
		this.startDate = startDate;
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
		return TimeUtils.fromLocalTimestamp(LocalDateTime.of(getStartDate(), getStartTime()));
	}
	
	@Transient
	public void setStartTimestamp(Instant timestamp)
	{
		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
		setStartDate(ldt.toLocalDate());
		setStartTime(ldt.toLocalTime());
	}
	
	
	public String getComment()
	{
		return comment;
	}
	
	public void setComment(String comment)
	{
		this.comment = comment;
	}
	
	
	public LocalDate getEndDate()
	{
		return endDate;
	}
	
	public void setEndDate(LocalDate endDate)
	{
		this.endDate = endDate;
	}
	
	public LocalTime getEndTime()
	{
		return endTime;
	}
	
	public void setEndTime(LocalTime endTime)
	{
		this.endTime = endTime;
	}
	
	@Transient
	public Instant getEndTimestamp()
	{
		LocalDate ed = getEndDate();
		LocalTime et = getEndTime();
		return ed == null || et == null ? null : TimeUtils.fromLocalTimestamp(LocalDateTime.of(ed, et));
	}
	
	@Transient
	public void setEndTimestamp(Instant timestamp)
	{
		if (timestamp == null)
		{
			setEndDate(null);
			setEndTime(null);
		}
		else
		{
  		LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
  		setEndDate(ldt.toLocalDate());
  		setEndTime(ldt.toLocalTime());
		}
	}
	
	
	public Instant getRemoved()
	{
		return removed;
	}
	
	public void setRemoved(Instant removed)
	{
		this.removed = removed;
	}
	
	
	public PageInfo toPageInfo()
	{
		return new PageInfo(new PageId(new BookId(part), name), getStartTimestamp(), getEndTimestamp(), getComment(), getRemoved());
	}
}