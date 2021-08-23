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
import java.util.Set;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Holds information about single (individual) test event stored in Cradle
 */
public class StoredTestEventSingle extends StoredTestEvent implements TestEventSingle
{
	private final Instant endTimestamp;
	private final boolean success;
	private final byte[] content;
	
	public StoredTestEventSingle(TestEventSingle event) throws CradleStorageException
	{
		super(event);
		
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		this.content = event.getContent();
		
		Set<StoredMessageId> eventMessages = event.getMessages();
		if (eventMessages != null)
			this.messages.addAll(eventMessages);
		
		TestEventUtils.validateTestEvent(this, true);
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
}
