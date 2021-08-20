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
import java.util.Objects;

import com.exactpro.cradle.books.BookId;
import com.exactpro.cradle.utils.CradleIdException;

/**
 * Holds ID of a test event stored in Cradle
 */
public class StoredTestEventId implements Serializable
{
	private static final long serialVersionUID = 6954746788528942942L;
	
	public static final String ID_PARTS_DELIMITER = ":";
	
	private final BookId bookId;
	private final String scope;
	private final Instant startTimestamp;
	private final String id;
	
	public StoredTestEventId(BookId bookId, String scope, Instant startTimestamp, String id)
	{
		this.bookId = bookId;
		this.scope = scope;
		this.startTimestamp = startTimestamp;
		this.id = id;
	}
	
	public static StoredTestEventId fromString(String id) throws CradleIdException
	{
		String[] parts = StoredTestEventIdUtils.splitParts(id);
		
		String uniqueId = StoredTestEventIdUtils.getId(parts);
		Instant timestamp = StoredTestEventIdUtils.getTimestamp(parts);
		String scope = StoredTestEventIdUtils.getScope(parts);
		BookId book = StoredTestEventIdUtils.getBook(parts);
		return new StoredTestEventId(book, scope, timestamp, uniqueId);
	}
	
	
	public BookId getBookId()
	{
		return bookId;
	}
	
	public String getScope()
	{
		return scope;
	}
	
	public Instant getStartTimestamp()
	{
		return startTimestamp;
	}
	
	public String getId()
	{
		return id;
	}
	
	
	@Override
	public String toString()
	{
		return bookId+ID_PARTS_DELIMITER+scope+ID_PARTS_DELIMITER+StoredTestEventIdUtils.timestampToString(startTimestamp)+ID_PARTS_DELIMITER+id;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(bookId, scope, startTimestamp, id);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StoredTestEventId other = (StoredTestEventId) obj;
		return Objects.equals(bookId, other.bookId)
				&& Objects.equals(scope, other.scope)
				&& Objects.equals(startTimestamp, other.startTimestamp)
				&& Objects.equals(id, other.id);
	}
}