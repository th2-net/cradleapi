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

package com.exactpro.cradle.cassandra.dao.cache;

import java.util.Objects;

public class CachedScope
{
	private final String book,
			scope;
	
	public CachedScope(String book, String scope)
	{
		this.book = book;
		this.scope = scope;
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(book, scope);
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
		CachedScope other = (CachedScope) obj;
		return Objects.equals(book, other.book) && Objects.equals(scope, other.scope);
	}

	@Override
	public String toString()
	{
		return "CachedScope [book=" + book + ", scope=" + scope + "]";
	}
	
	
	public String getBook()
	{
		return book;
	}
	
	public String getScope()
	{
		return scope;
	}
}
