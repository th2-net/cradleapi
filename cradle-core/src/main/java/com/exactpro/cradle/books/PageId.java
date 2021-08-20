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

package com.exactpro.cradle.books;

import java.util.Objects;

/**
 * Identifier of the page within a book
 */
public class PageId
{
	public static final String DELIMITER = ":";
	
	private final BookId bookId;
	private final String name;
	
	public PageId(BookId bookId, String pageName)
	{
		this.bookId = bookId;
		this.name = pageName;
	}
	
	
	public BookId getBookId()
	{
		return bookId;
	}
	
	public String getName()
	{
		return name;
	}
	
	
	@Override
	public int hashCode()
	{
		return Objects.hash(bookId, name);
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
		return Objects.equals(bookId, other.bookId) && Objects.equals(name, other.name);
	}
	
	
	@Override
	public String toString()
	{
		return bookId+DELIMITER+name;
	}
}
