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

package com.exactpro.cradle;

import java.time.Instant;
import java.util.Objects;

/**
 * Information about book's page
 */
public class PageInfo
{
	private final PageId id;
	private final Instant started;
	private final Instant ended;
	private final String name;
	private final String comment;
	private final Instant updated;
	private final Instant removed;

	public PageInfo(PageId id, Instant started, Instant ended, String name, String comment, Instant updated, Instant removed)
	{
		this.id = id;
		this.started = started;
		this.ended = ended;
		this.name = name;
		this.comment = comment;
		this.updated = updated;
		this.removed = removed;
	}

	public PageInfo(PageId id, Instant started, Instant ended, String name, String comment)
	{
		this.id = id;
		this.started = started;
		this.ended = ended;
		this.name = name;
		this.comment = comment;
		this.updated = null;
		this.removed = null;
	}
	
	
	public PageId getId()
	{
		return id;
	}
	/**
	 * @deprecated name has been moved to {@link PageId#getStart()}
	 */
	@Deprecated
	public Instant getStarted()
	{
		return started;
	}
	
	public Instant getEnded()
	{
		return ended;
	}

	public String getName() {
		return name;
	}

	public String getComment()
	{
		return comment;
	}

	public Instant getUpdated() {
		return updated;
	}

	public Instant getRemoved() {
		return removed;
	}

	public static PageInfo ended(PageInfo page, Instant endTimestamp)
	{
		return page == null ? null : new PageInfo(page.getId(), page.getId().getStart(), endTimestamp, page.getName(), page.getComment(), page.getUpdated(), page.getRemoved());
	}

	public boolean isValidFor(Instant timestamp) {
	    return (started == null || !started.isAfter(timestamp)) &&
				(ended == null || ended.isAfter(timestamp));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PageInfo pageInfo = (PageInfo) o;
		return getId().equals(pageInfo.getId())
				&& Objects.equals(getEnded(), pageInfo.getEnded())
				&& Objects.equals(getComment(), pageInfo.getComment())
				&& Objects.equals(getUpdated(), pageInfo.getUpdated())
				&& Objects.equals(getRemoved(), pageInfo.getRemoved());
	}

	@Override
	public String toString() {
		return "PageInfo{" +
				"id=" + id +
				", ended=" + ended +
				", name='" + name + '\'' +
				", comment='" + comment + '\'' +
				", updated=" + updated +
				", removed=" + removed +
				'}';
	}
}
