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
import java.util.StringJoiner;

public class CachedSession
{
	private final String part,
			sessionAlias;

	public CachedSession(String part, String sessionAlias)
	{
		this.part = part;
		this.sessionAlias = sessionAlias;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(part, sessionAlias);
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
		CachedSession other = (CachedSession) obj;
		return Objects.equals(part, other.part)
				&& Objects.equals(sessionAlias, other.sessionAlias);
	}

	@Override
	public String toString()
	{
		return new StringJoiner(", ", CachedSession.class.getSimpleName() + "[", "]")
				.add("part='" + part + "'")
				.add("sessionAlias='" + sessionAlias + "'")
				.toString();
	}
	
	public String getPart()
	{
		return part;
	}
	
	public String getSessionAlias()
	{
		return sessionAlias;
	}
}
