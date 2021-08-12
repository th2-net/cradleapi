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

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.EntityConverter;
import com.exactpro.cradle.cassandra.retries.PagingSupplies;

/**
 * Wrapper for asynchronous paging iterable that converts retrieved entities into Cradle objects
 * @param <R> - class of objects to get while using the wrapper
 * @param <E> - class of entities obtained from Cassandra
 */
public abstract class ConvertingPagedIterator<R, E> implements Iterator<R>
{
	private static final Logger logger = LoggerFactory.getLogger(ConvertingPagedIterator.class);
	
	private final PagedIterator<E> it;
	
	public ConvertingPagedIterator(MappedAsyncPagingIterable<E> rows, PagingSupplies pagingSupplies, EntityConverter<E> converter, String queryInfo)
	{
		this.it = new PagedIterator<>(rows, pagingSupplies, converter, queryInfo);
	}
	
	
	protected abstract R convertEntity(E entity) throws IOException;
	
	
	@Override
	public boolean hasNext()
	{
		return it.hasNext();
	}
	
	@Override
	public R next()
	{
		E entity = it.next();
		
		logger.trace("Converting entity");
		try
		{
			return convertEntity(entity);
		}
		catch (IOException e)
		{
			throw new RuntimeException("Error while getting next data row", e);
		}
	}
}
