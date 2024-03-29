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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;

/**
 * Interface to access all metadata fields of test event
 */
public interface TestEvent
{
	StoredTestEventId getId();
	String getName();
	String getType();
	StoredTestEventId getParentId();
	Instant getEndTimestamp();
	boolean isSuccess();
	Set<StoredMessageId> getMessages();
	
	public default BookId getBookId()
	{
		StoredTestEventId id = getId();
		return id != null ? id.getBookId() : null;
	}
	
	public default String getScope()
	{
		StoredTestEventId id = getId();
		return id != null ? id.getScope() : null;
	}
	
	public default Instant getStartTimestamp()
	{
		StoredTestEventId id = getId();
		return id != null ? id.getStartTimestamp() : null;
	}
}
