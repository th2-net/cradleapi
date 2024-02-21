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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.utils.TimeUtils;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_PAGE_REMOVE_TIME;

@SuppressWarnings("DefaultAnnotationParam")
@Entity
@CqlName(PageEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class PageEntity {
	public static final String TABLE_NAME = "pages";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_START_DATE = "start_date";
	public static final String FIELD_START_TIME = "start_time";
	public static final String FIELD_NAME = "name";
	public static final String FIELD_COMMENT = "comment";
	public static final String FIELD_END_DATE = "end_date";
	public static final String FIELD_END_TIME = "end_time";
	public static final String FIELD_UPDATED = "updated";
	public static final String FIELD_REMOVED = "removed";

	@PartitionKey(0)
	@CqlName(FIELD_BOOK)
	private final String book;

	@ClusteringColumn(0)
	@CqlName(FIELD_START_DATE)
	private final LocalDate startDate;

	@ClusteringColumn(1)
	@CqlName(FIELD_START_TIME)
	private final LocalTime startTime;

	@CqlName(FIELD_NAME)
	private final String name;

	@CqlName(FIELD_COMMENT)
	private final String comment;

	@CqlName(FIELD_END_DATE)
	private final LocalDate endDate;

	@CqlName(FIELD_END_TIME)
	private final LocalTime endTime;

	@CqlName(FIELD_UPDATED)
	private final Instant updated;

	@CqlName(FIELD_REMOVED)
	private final Instant removed;


	public PageEntity(String book, LocalDate startDate, LocalTime startTime, String name, String comment, LocalDate endDate, LocalTime endTime, Instant updated, Instant removed) {
		this.book = book;
		this.startDate = startDate;
		this.startTime = startTime;
		this.name = name;
		this.comment = comment;
		this.endDate = endDate;
		this.endTime = endTime;
		this.updated = updated;
		this.removed = removed;
	}

	public PageEntity(String book, String name, Instant started, String comment, Instant ended, Instant updated)	{

		LocalDateTime startedLdt = TimeUtils.toLocalTimestamp(started);

		this.book = book;
		this.name = name;
		this.startDate = startedLdt.toLocalDate();
		this.startTime = startedLdt.toLocalTime();
		this.comment = comment;
		this.updated = updated == null ? started : updated;
		this.removed = DEFAULT_PAGE_REMOVE_TIME;

		if (ended != null) {
			LocalDateTime endedLdt = TimeUtils.toLocalTimestamp(ended);
			this.endDate = endedLdt.toLocalDate();
			this.endTime = endedLdt.toLocalTime();
		}  else {
			this.endDate = null;
			this.endTime = null;
		}
	}

	public PageEntity(PageInfo pageInfo) {
		this(pageInfo.getId().getBookId().getName(), pageInfo.getName(), pageInfo.getStarted(), pageInfo.getComment(), pageInfo.getEnded(), pageInfo.getUpdated());
	}


	public String getBook() {
		return book;
	}
	public String getName()	{
		return name;
	}
	public LocalDate getStartDate()	{
		return startDate;
	}
	public LocalTime getStartTime()	{
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
	public Instant getUpdated() {
		return updated;
	}
	public Instant getRemoved() {
		return removed;
	}

	public PageInfo toPageInfo() {
		Instant start = TimeUtils.toInstant(getStartDate(), getStartTime());
		return new PageInfo(new PageId(new BookId(book), start, name),
				TimeUtils.toInstant(getEndDate(), getEndTime()),
				getComment(),
				getUpdated(),
				getRemoved());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PageEntity)) return false;
		PageEntity that = (PageEntity) o;

		return Objects.equals(getBook(), that.getBook())
				&& Objects.equals(getStartDate(), that.getStartDate())
				&& Objects.equals(getStartTime(), that.getStartTime())
				&& Objects.equals(getName(), that.getName())
				&& Objects.equals(getComment(), that.getComment())
				&& Objects.equals(getEndDate(), that.getEndDate())
				&& Objects.equals(getEndTime(), that.getEndTime())
				&& Objects.equals(getUpdated(), that.getUpdated())
				&& Objects.equals(getRemoved(), that.getRemoved());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBook(),
				getStartDate(),
				getStartTime(),
				getName(),
				getComment(),
				getEndDate(),
				getEndTime(),
				getUpdated(),
				getRemoved());
	}
}