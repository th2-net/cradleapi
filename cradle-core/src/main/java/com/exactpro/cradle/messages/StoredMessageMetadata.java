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

package com.exactpro.cradle.messages;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Unmodifiable metadata of message stored in Cradle
 */
public class StoredMessageMetadata implements Serializable
{
	private static final long serialVersionUID = -3397537024211430427L;
	
	protected final Map<String, String> data;
	
	public StoredMessageMetadata()
	{
		this(new HashMap<>());
	}

	protected StoredMessageMetadata(Map<String, String> data)
	{
		this.data = data;
	}
	
	public StoredMessageMetadata(StoredMessageMetadata metadata)
	{
		this();
		this.data.putAll(metadata.data);
	}
	
	public static StoredMessageMetadata empty() {
		return new StoredMessageMetadata(Collections.emptyMap());
	}
	
	
	@Override
	public String toString()
	{
		return data.toString();
	}
	
	
	public String get(String key)
	{
		return data.get(key);
	}
	
	public Set<String> getKeys()
	{
		return Collections.unmodifiableSet(data.keySet());
	}
	
	public Map<String, String> toMap()
	{
		return Collections.unmodifiableMap(data);
	}
	
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		return result;
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
		StoredMessageMetadata other = (StoredMessageMetadata) obj;
		if (data == null)
		{
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}
}
