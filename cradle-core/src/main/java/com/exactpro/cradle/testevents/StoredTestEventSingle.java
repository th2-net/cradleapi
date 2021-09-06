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

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;

/**
 * Holds information about single (individual) test event stored in Cradle
 */
public class StoredTestEventSingle extends StoredTestEvent implements TestEventSingle
{
	private final Instant endTimestamp;
	private final boolean success;
	private final Set<StoredMessageId> messages;
	private final byte[] content;
	
	public StoredTestEventSingle(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
			Instant endTimestamp, boolean success, byte[] eventContent, Set<StoredMessageId> eventMessages, PageId pageId, String error)
	{
		super(id, name, type, parentId, pageId, error);
		
		this.endTimestamp = endTimestamp;
		this.success = success;
		
		if (eventContent == null)
			this.content = null;
		else
		{
			this.content = new byte[eventContent.length];
			System.arraycopy(eventContent, 0, this.content, 0, this.content.length);
		}
		
		this.messages = eventMessages != null && eventMessages.size() > 0 ? Collections.unmodifiableSet(new HashSet<>(eventMessages)) : null;
	}
	
	public StoredTestEventSingle(TestEventSingle event, PageId pageId)
	{
		this(event.getId(), event.getName(), event.getType(), event.getParentId(),
				event.getEndTimestamp(), event.isSuccess(), event.getContent(), event.getMessages(), pageId, null);
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
}
