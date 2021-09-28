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

package com.exactpro.cradle.cassandra;

import java.time.Instant;

import com.exactpro.cradle.BookToAdd;

public class CassandraBookToAdd extends BookToAdd
{
	private final String keyspace;
	
	public CassandraBookToAdd(String name, Instant created, String firstPageName, String keyspace)
	{
		super(name, created, firstPageName);
		this.keyspace = keyspace;
	}
	
	
	public String getKeyspace()
	{
		return keyspace;
	}
}
