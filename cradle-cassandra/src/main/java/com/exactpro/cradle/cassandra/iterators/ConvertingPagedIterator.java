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
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;

/**
 * Wrapper for asynchronous paging iterable that converts retrieved entities into Cradle objects
 * @param <R> - class of objects to get while using the wrapper
 * @param <E> - class of entities obtained from Cassandra
 */
public class ConvertingPagedIterator<R, E> implements Iterator<R>
{
	private static final Logger logger = LoggerFactory.getLogger(ConvertingPagedIterator.class);
	
	private final PagedIterator<E> it;
	private final Function<E, R> converter;
	
	public ConvertingPagedIterator(MappedAsyncPagingIterable<E> rows, Function<E, R> converter)
	{
		this.it = new PagedIterator<>(rows);
		this.converter = converter;
	}
	
	
	@Override
	public boolean hasNext()
	{
		return it.hasNext();
	}
	
	@Override
	public R next()
	{
		E entity = it.next();
		
		//TODO: implement handling of multi-chunked entities
		
		logger.trace("Converting entity");
		return converter.apply(entity);
	}
}
