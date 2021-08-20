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

import java.util.Set;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.testevents.StoredTestEvent;

public class EventEntityData
{
	private StoredTestEvent event;
	private PageId pageId;
	private int chunk;
	private boolean lastChunk,
			compressed;
	private byte[] content;
	private Set<String> messages;
	
	public EventEntityData()
	{
	}
	
	public EventEntityData(StoredTestEvent event, PageId pageId, byte[] content, boolean compressed)
	{
		this.event = event;
		this.pageId = pageId;
		this.chunk = 0;
		this.lastChunk = true;
		this.content = content;
		this.compressed = compressed;
		//TODO: this.messages = event.getMessages();
	}
	
	public EventEntityData(StoredTestEvent event, PageId pageId, int chunk, boolean lastChunk, byte[] content, boolean compressed, Set<String> messages)
	{
		this.event = event;
		this.pageId = pageId;
		this.chunk = chunk;
		this.lastChunk = lastChunk;
		this.content = content;
		this.compressed = compressed;
		this.messages = messages;
	}
	
	
	public StoredTestEvent getEvent()
	{
		return event;
	}
	
	public void setEvent(StoredTestEvent event)
	{
		this.event = event;
	}
	
	
	public PageId getPageId()
	{
		return pageId;
	}
	
	public void setPageId(PageId pageId)
	{
		this.pageId = pageId;
	}
	
	
	public int getChunk()
	{
		return chunk;
	}
	
	public void setChunk(int chunk)
	{
		this.chunk = chunk;
	}
	
	
	public boolean isLastChunk()
	{
		return lastChunk;
	}
	
	public void setLastChunk(boolean lastChunk)
	{
		this.lastChunk = lastChunk;
	}
	
	
	public byte[] getContent()
	{
		return content;
	}
	
	public void setContent(byte[] content)
	{
		this.content = content;
	}
	
	
	public boolean isCompressed()
	{
		return compressed;
	}
	
	public void setCompressed(boolean compressed)
	{
		this.compressed = compressed;
	}
	
	
	public Set<String> getMessages()
	{
		return messages;
	}
	
	public void setMessages(Set<String> messages)
	{
		this.messages = messages;
	}
}