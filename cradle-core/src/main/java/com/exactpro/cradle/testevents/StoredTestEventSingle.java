/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import java.util.Collection;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Holds information about single (individual) test event stored in Cradle
 */
public class StoredTestEventSingle extends MinimalStoredTestEvent implements StoredTestEventWithContent
{
	private final Instant startTimestamp,
			endTimestamp;
	private final boolean success;
	private final byte[] content;
	private final Collection<StoredMessageId> messageIds;

	public StoredTestEventSingle(StoredTestEventWithContent event) throws CradleStorageException
	{
		super(event);
		this.startTimestamp = event.getStartTimestamp();
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		this.content = event.getContent();
		this.messageIds = event.getMessageIds();
	}
	
	
	@Override
	public Instant getStartTimestamp()
	{
		return startTimestamp;
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
	public byte[] getContent()
	{
		return content;
	}

	@Override
	public Collection<StoredMessageId> getMessageIds()
	{
		return messageIds;
	}
}
