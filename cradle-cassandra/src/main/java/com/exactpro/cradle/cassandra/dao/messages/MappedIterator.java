/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.messages;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MappedIterator<S, T> implements Iterator<T>
{
	private final int limit;
	private final AtomicInteger returned;
	private final Iterator<S> sourceIterator;
	private Iterator<T> targetIterator;

	public MappedIterator(Iterator<S> sourceIterator, int limit, AtomicInteger returned)
	{
		this.sourceIterator = sourceIterator;
		this.limit = limit;
		this.returned = returned;
	}
	
	abstract Iterator<T> createTargetIterator(Iterator<S> sourceIterator);

	@Override
	public boolean hasNext()
	{
		if(targetIterator == null) {
			targetIterator = createTargetIterator(sourceIterator);
		}
		return (limit <= 0 || returned.get() < limit) && targetIterator.hasNext();
	}

	@Override
	public T next()
	{
		if(targetIterator == null) {
			targetIterator = createTargetIterator(sourceIterator);
		}
		returned.incrementAndGet();
		return targetIterator.next();
	}
}
