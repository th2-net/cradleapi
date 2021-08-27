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

package com.exactpro.cradle.cassandra.resultset;

import java.util.Iterator;

import com.exactpro.cradle.resultset.CradleResultSet;

/**
 * Iterator for request results that performs additional queries to get iterators for next partitions that fall into the request
 * @param <T> class of iterator item
 */
public class CassandraCradleResultSet<T> implements CradleResultSet<T>
{
	private final IteratorProvider<T> provider;
	private Iterator<T> it;
	
	public CassandraCradleResultSet(Iterator<T> iterator, IteratorProvider<T> nextIteratorProvider)
	{
		this.it = iterator;
		this.provider = nextIteratorProvider;
	}
	
	@Override
	public boolean hasNext()
	{
		if (it == null)
			return false;
		
		if (it.hasNext())
			return true;
		
		if (provider == null)  //Current iterator drained, next one cannot be requested
			return false;
		
		do
		{
			try
			{
				it = provider.nextIterator().get();
			}
			catch (Exception e)
			{
				throw new RuntimeException("Error while getting next iterator for '"+provider.getRequestInfo()+"'", e);
			}
			
			if (it == null)
				return false;
		}
		while (!it.hasNext());
		return true;
	}

	@Override
	public T next()
	{
		return it != null ? it.next() : null;
	}
}
