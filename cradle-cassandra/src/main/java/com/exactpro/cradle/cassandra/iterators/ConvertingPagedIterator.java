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

package com.exactpro.cradle.cassandra.iterators;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;

/**
 * Wrapper for asynchronous paging iterable that converts retrieved entities into Cradle objects
 * @param <R> - class of objects to get while using the wrapper
 * @param <E> - class of entities obtained from Cassandra
 */
public class ConvertingPagedIterator<R, E> implements Iterator<R>
{
	private final PagedIterator<E> it;
	private final int limit;
	private final AtomicInteger returned;
	private final Function<E, R> converter;

	public ConvertingPagedIterator(MappedAsyncPagingIterable<E> rows, SelectQueryExecutor selectExecutor,
			int limit, AtomicInteger returned, Function<E, R> converter, Function<Row, E> mapper, String queryInfo)
	{
		this.it = new PagedIterator<>(rows, selectExecutor, mapper, queryInfo);
		this.limit = limit;
		this.returned = returned;
		this.converter = converter;
	}
	
	
	@Override
	public boolean hasNext()
	{
		return (limit <= 0 || returned.get() < limit) && it.hasNext();
	}
	
	@Override
	public R next()
	{
		if (limit > 0 && returned.get() >= limit)
			return null;
		
		E entity = it.next();
		
		returned.incrementAndGet();
		return converter.apply(entity);
	}
}
