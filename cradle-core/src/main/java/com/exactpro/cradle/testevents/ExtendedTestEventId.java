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

package com.exactpro.cradle.testevents;

/**
 * Identifies test event even if it is stored within a batch
 */
public class ExtendedTestEventId
{
	private final StoredTestEventId eventId,
			batchId;
	
	public ExtendedTestEventId(StoredTestEventId eventId, StoredTestEventId batchId)
	{
		this.eventId = eventId;
		this.batchId = batchId;
	}
	
	@Override
	public String toString()
	{
		if (batchId != null)
			return batchId+":"+eventId;
		return eventId.toString();
	}
	
	
	public StoredTestEventId getEventId()
	{
		return eventId;
	}
	
	public StoredTestEventId getBatchId()
	{
		return batchId;
	}
}
