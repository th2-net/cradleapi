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

import static com.exactpro.cradle.cassandra.StorageConstants.*;

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
	@PartitionKey(0)
	@CqlName(BOOK)
	private String book;
	
	@ClusteringColumn(0)
	@CqlName(START_DATE)
	private LocalDate startDate;
	
	@ClusteringColumn(1)
	@CqlName(START_TIME)
	private LocalTime startTime;
	
	@CqlName(NAME)
	private String name;
	
	@CqlName(COMMENT)
	private String comment;
	
	@CqlName(END_DATE)
	private LocalDate endDate;
	
	@CqlName(END_TIME)
	private LocalTime endTime;
	
	
	public PageEntity()
	{
	}
	
	public PageEntity(String book, String name, Instant started, String comment, Instant ended)
	{
		LocalDateTime startedLdt = TimeUtils.toLocalTimestamp(started);
		
		this.book = book;
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


	public String getBook()
	{
		return book;
	}

	public void setBook(String book)
	{
		this.book = book;
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
	
	
	public PageInfo toPageInfo()
	{
		return new PageInfo(new PageId(new BookId(book), name), getStartTimestamp(), getEndTimestamp(), getComment());
	}
}