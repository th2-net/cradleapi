/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;

/**
 * Holds information about one test event stored in batch of events ({@link TestEventBatch})
 */
//This class is not a child of StoredTestEventSingle because it is used in serialization which doesn't work with super() constructor with arguments for final fields
public class BatchedStoredTestEvent implements TestEventSingle, Serializable
{
	private static final long serialVersionUID = -1350827714114261304L;
	
	private final StoredTestEventId id;
	private final String name,
			type;
	private final StoredTestEventId parentId;
	private final Instant endTimestamp;
	private final boolean success;
	private final Set<StoredMessageId> messages;
	private final byte[] content;
	
	private final transient TestEventBatch batch;
	private final transient PageId pageId;
	
	public BatchedStoredTestEvent(TestEventSingle event, TestEventBatch batch, PageId pageId)
	{
		this.id = event.getId();
		this.name = event.getName();
		this.type = event.getType();
		this.parentId = event.getParentId();
		
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		
		byte[] eventContent = event.getContent();
		if (eventContent == null)
			this.content = null;
		else
		{
			this.content = new byte[eventContent.length];
			System.arraycopy(eventContent, 0, this.content, 0, this.content.length);
		}
		
		Set<StoredMessageId> eventMessages = event.getMessages();
		this.messages = eventMessages != null && eventMessages.size() > 0 ? Collections.unmodifiableSet(new HashSet<>(eventMessages)) : null;
		
		this.batch = batch;
		this.pageId = pageId;
	}
	
	
	@Override
	public StoredTestEventId getId()
	{
		return id;
	}
	
	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public String getType()
	{
		return type;
	}
	
	@Override
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	@Override
	public Instant getEndTimestamp()
	{
		return endTimestamp;
	}
	
	@Override
	public boolean isSuccess()
	{
		return success;
	}
	
	@Override
	public Set<StoredMessageId> getMessages()
	{
		return messages;
	}
	
	@Override
	public byte[] getContent()
	{
		return content;
	}
	

	public PageId getPageId()
	{
		return pageId;
	}
	
	public StoredTestEventId getBatchId()
	{
		return batch.getId();
	}
	
	public boolean hasChildren()
	{
		return batch.hasChildren(getId());
	}
	
	public Collection<BatchedStoredTestEvent> getChildren()
	{
		return batch.getChildren(getId());
	}
}
