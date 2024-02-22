/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.utils.TimeUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

@Entity
@CqlName(PageNameEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class PageNameEntity {
	public static final String TABLE_NAME = "page_names";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_NAME = "name";
	public static final String FIELD_START_DATE = "start_date";
	public static final String FIELD_START_TIME = "start_time";
	public static final String FIELD_COMMENT = "comment";
	public static final String FIELD_END_DATE = "end_date";
	public static final String FIELD_END_TIME = "end_time";


	@PartitionKey(0)
	@CqlName(FIELD_BOOK)
	private final String book;
	
	@PartitionKey(1)
	@CqlName(FIELD_NAME)
	private final String name;
	
	@CqlName(FIELD_START_DATE)
	private final LocalDate startDate;
	
	@CqlName(FIELD_START_TIME)
	private final LocalTime startTime;
	
	@CqlName(FIELD_COMMENT)
	private final String comment;
	
	@CqlName(FIELD_END_DATE)
	private final LocalDate endDate;
	
	@CqlName(FIELD_END_TIME)
	private final LocalTime endTime;

	public PageNameEntity(String book, String name, LocalDate startDate, LocalTime startTime, String comment, LocalDate endDate, LocalTime endTime) {
		this.book = book;
		this.name = name;
		this.startDate = startDate;
		this.startTime = startTime;
		this.comment = comment;
		this.endDate = endDate;
		this.endTime = endTime;
	}

	public PageNameEntity(String book, String name, Instant started, String comment, Instant ended)	{
		LocalDateTime startedLdt = TimeUtils.toLocalTimestamp(started);
		
		this.book = book;
		this.name = name;
		this.startDate = startedLdt.toLocalDate();
		this.startTime = startedLdt.toLocalTime();
		this.comment = comment;
		
		if (ended != null) {
			LocalDateTime endedLdt = TimeUtils.toLocalTimestamp(ended);
			this.endDate = endedLdt.toLocalDate();
			this.endTime = endedLdt.toLocalTime();
		} else {
			this.endDate = null;
			this.endTime = null;
		}
	}
	
	public PageNameEntity(PageInfo pageInfo) {
		this(pageInfo.getBookName(), pageInfo.getName(), pageInfo.getStarted(), pageInfo.getComment(), pageInfo.getEnded());
	}
	
	
	public String getBook()	{
		return book;
	}
	public String getName() {
		return name;
	}
	public LocalDate getStartDate()	{
		return startDate;
	}
	public LocalTime getStartTime() {
		return startTime;
	}
	public String getComment() {
		return comment;
	}
	public LocalDate getEndDate() {
		return endDate;
	}
	public LocalTime getEndTime() {
		return endTime;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PageNameEntity)) return false;
		PageNameEntity that = (PageNameEntity) o;

		return Objects.equals(getBook(), that.getBook())
				&& Objects.equals(getName(), that.getName())
				&& Objects.equals(getStartDate(), that.getStartDate())
				&& Objects.equals(getStartTime(), that.getStartTime())
				&& Objects.equals(getComment(), that.getComment())
				&& Objects.equals(getEndDate(), that.getEndDate())
				&& Objects.equals(getEndTime(), that.getEndTime());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBook(),
				getName(),
				getStartDate(),
				getStartTime(),
				getComment(),
				getEndDate(),
				getEndTime());
	}
}