/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.EscapeUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Holds ID of a test event stored in Cradle
 */
public class StoredTestEventId implements Serializable
{
	private static final long serialVersionUID = 6954746788528942942L;
	public static final String ID_PARTS_DELIMITER = EscapeUtils.DELIMITER_STR;
	
	private @Nonnull final BookId bookId;
	private @Nonnull final String scope;
	private @Nonnull final Instant startTimestamp;
	private @Nonnull final String id;
	private final int hash;
	
	public StoredTestEventId(@Nonnull BookId bookId,
							 @Nonnull String scope,
							 @Nonnull Instant startTimestamp,
							 @Nonnull String id) {
		this.bookId = requireNonNull(bookId, "Book id can't be null");
		this.startTimestamp = requireNonNull(startTimestamp, "Event id must have a scope");
		if (isEmpty(scope)) throw new IllegalArgumentException("Scope can't be null or empty");
		this.scope = scope;
		if (isEmpty(id)) throw new IllegalArgumentException("Id can't be null or empty");
		this.id = id;
		this.hash = Objects.hash(bookId, scope, startTimestamp, id);
	}
	
	public static StoredTestEventId fromString(String id) throws CradleIdException
	{
		List<String> parts = StoredTestEventIdUtils.splitParts(id);
		
		String uniqueId = StoredTestEventIdUtils.getId(parts);
		Instant timestamp = StoredTestEventIdUtils.getTimestamp(parts);
		String scope = StoredTestEventIdUtils.getScope(parts);
		BookId book = StoredTestEventIdUtils.getBook(parts);
		return new StoredTestEventId(book, scope, timestamp, uniqueId);
	}
	
	
	@Nonnull
	public BookId getBookId()
	{
		return bookId;
	}
	
	@Nonnull
	public String getScope()
	{
		return scope;
	}
	
	@Nonnull
	public Instant getStartTimestamp()
	{
		return startTimestamp;
	}
	
	@Nonnull
	public String getId()
	{
		return id;
	}
	
	
	@Override
	public String toString()
	{
		return StringUtils.joinWith(ID_PARTS_DELIMITER,
				EscapeUtils.escape(bookId.toString()),
				EscapeUtils.escape(scope),
				StoredTestEventIdUtils.timestampToString(startTimestamp),
				EscapeUtils.escape(id));
	}

	@Override
	public int hashCode()
	{
		return hash;
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
		return Objects.equals(id, other.id)
				&& Objects.equals(startTimestamp, other.startTimestamp)
				&& Objects.equals(scope, other.scope)
				&& Objects.equals(bookId, other.bookId);
	}
}