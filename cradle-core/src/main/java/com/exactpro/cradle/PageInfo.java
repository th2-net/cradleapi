/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

/**
 * Information about book's page
 */
public class PageInfo
{
	private final PageId id;
	private final Instant started,
			ended;
	private final String comment;
	private final Instant removed;

	public PageInfo(PageId id, Instant started, Instant ended, String comment, Instant removed)
	{
		this.id = id;
		this.started = started;
		this.ended = ended;
		this.comment = comment;
		this.removed = removed;
	}

	public PageInfo(PageId id, Instant started, Instant ended, String comment)
	{
		this.id = id;
		this.started = started;
		this.ended = ended;
		this.comment = comment;
		this.removed = null;
	}
	
	
	public PageId getId()
	{
		return id;
	}
	
	public Instant getStarted()
	{
		return started;
	}
	
	public Instant getEnded()
	{
		return ended;
	}
	
	public String getComment()
	{
		return comment;
	}

	public Instant getRemoved() {
		return removed;
	}

	public static PageInfo ended(PageInfo page, Instant endTimestamp)
	{
		return page == null ? null : new PageInfo(page.getId(), page.getStarted(), endTimestamp, page.getComment(), page.getRemoved());
	}
}
