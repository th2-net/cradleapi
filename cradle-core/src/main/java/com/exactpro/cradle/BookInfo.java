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
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Information about a book
 */
public class BookInfo
{
	private final BookId id;
	private final String fullName,
			desc;
	private final Instant created;
	private final Map<PageId, PageInfo> pages;
	private final TreeMap<Instant, PageInfo> orderedPages;
	
	public BookInfo(BookId id, String fullName, String desc, Instant created, Collection<PageInfo> pages) throws CradleStorageException
	{
		this.id = id;
		this.fullName = fullName;
		this.desc = desc;
		this.created = created;
		this.pages = new ConcurrentHashMap<>();
		this.orderedPages = new TreeMap<>();
		
		if (pages == null)
			return;
		
		PageInfo notEndedPage = null;
		for (PageInfo p : pages)
		{
			this.pages.put(p.getId(), p);
			this.orderedPages.put(p.getStarted(), p);
			if (p.getEnded() == null)
			{
				if (notEndedPage != null)
					throw new CradleStorageException("Inconsistent state of book '"+id+"': "
							+ "page '"+ notEndedPage.getId().getName()+"' is not ended, "
							+ "but '"+p.getId().getName()+"' is not ended as well");
				notEndedPage = p;
			}
		}
	}
	
	
	public BookId getId()
	{
		return id;
	}
	
	public String getFullName()
	{
		return fullName;
	}
	
	public String getDesc()
	{
		return desc;
	}
	
	public Instant getCreated()
	{
		return created;
	}
	
	public Collection<PageInfo> getPages()
	{
		return Collections.unmodifiableCollection(orderedPages.values());
	}
	
	public PageInfo getFirstPage()
	{
		return orderedPages.size() > 0 ? orderedPages.firstEntry().getValue() : null;
	}
	
	public PageInfo getLastPage()
	{
		return orderedPages.size() > 0 ? orderedPages.lastEntry().getValue() : null;
	}
	
	public PageInfo getPage(PageId pageId)
	{
		return pages.get(pageId);
	}
	
	public PageInfo findPage(Instant timestamp)
	{
		Entry<Instant, PageInfo> result = orderedPages.floorEntry(timestamp);
		return result != null ? result.getValue() : null;
	}
	
	public PageInfo getNextPage(Instant startTimestamp)
	{
		Entry<Instant, PageInfo> result = orderedPages.ceilingEntry(startTimestamp.plus(1, ChronoUnit.NANOS));
		return result != null ? result.getValue() : null;
	}
	
	public PageInfo getPreviousPage(Instant startTimestamp)
	{
		Entry<Instant, PageInfo> result = orderedPages.floorEntry(startTimestamp.minus(1, ChronoUnit.NANOS));
		return result != null ? result.getValue() : null;
	}
	
	
	void removePage(PageId pageId)
	{
		PageInfo pageInfo = pages.remove(pageId);
		if (pageInfo != null)
			orderedPages.remove(pageInfo.getStarted());
	}
	
	void addPage(PageInfo page)
	{
		pages.put(page.getId(), page);
		orderedPages.put(page.getStarted(), page);
	}
}