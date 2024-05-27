/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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
import java.util.Objects;

import com.exactpro.cradle.utils.EscapeUtils;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Identifier of the page within a book
 */
public class PageId
{
	public static final String DELIMITER = EscapeUtils.DELIMITER_STR;
	
	private final @Nonnull BookId bookId;
	private final @Nonnull Instant start;
	private final @Nonnull String name;

	public PageId(BookId bookId, Instant start, String pageName)
	{
		this.bookId = requireNonNull(bookId, "Book id can't be null");
		this.start = requireNonNull(start, "Start timestamp can't be null");
		this.name = requireNonNull(pageName, "Page name can't be null");
	}
	
	
	@Nonnull
	public BookId getBookId()
	{
		return bookId;
	}

	@Nonnull
	public Instant getStart() {
		return start;
	}

	@Nonnull
	public String getName()
	{
		return name;
	}
	
	
	@Override
	public int hashCode()
	{
		return Objects.hash(bookId, start, name);
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
		PageId other = (PageId) obj;
		return Objects.equals(bookId, other.bookId) &&
				Objects.equals(start, other.start) &&
				Objects.equals(name, other.name);
	}
	
	
	@Override
	public String toString()
	{
		return EscapeUtils.escape(bookId.toString())+DELIMITER+
				EscapeUtils.escape(start.toString())+DELIMITER+
				EscapeUtils.escape(name);
	}
}
