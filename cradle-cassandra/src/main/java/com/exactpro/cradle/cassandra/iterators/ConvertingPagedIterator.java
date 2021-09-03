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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.cassandra.dao.CradleEntity;

/**
 * Wrapper for asynchronous paging iterable that converts retrieved entities into Cradle objects
 * @param <R> - class of objects to get while using the wrapper
 * @param <E> - class of entities obtained from Cassandra
 */
public class ConvertingPagedIterator<R, E> implements Iterator<R>
{
	private static final Logger logger = LoggerFactory.getLogger(ConvertingPagedIterator.class);
	
	private final PagedIterator<E> it;
	private final int limit;
	private final AtomicInteger returned;
	private final Function<List<E>, R> converter;
	private E bufferedNext;
	
	public ConvertingPagedIterator(MappedAsyncPagingIterable<E> rows, int limit, AtomicInteger returned, Function<List<E>, R> converter)
	{
		this.it = new PagedIterator<>(rows);
		this.limit = limit;
		this.returned = returned;
		this.converter = converter;
	}
	
	
	@Override
	public boolean hasNext()
	{
		return (limit <= 0 || returned.get() < limit) && (bufferedNext != null || it.hasNext());
	}
	
	@Override
	public R next()
	{
		if (limit > 0 && returned.get() >= limit)
			return null;
		
		E entity;
		if (bufferedNext != null)
		{
			entity = bufferedNext;
			bufferedNext = null;
		}
		else
			entity = it.next();
		
		List<E> chunks = getChunks(entity);
		
		logger.trace("Converting {} chunk(s)", chunks.size());
		returned.incrementAndGet();
		return converter.apply(chunks);
	}
	
	
	private List<E> getChunks(E entity)
	{
		if (!(entity instanceof CradleEntity))
			return Collections.singletonList(entity);
		
		CradleEntity ce = (CradleEntity)entity;
		if (ce.isLastChunk())
			return Collections.singletonList(entity);
		
		List<E> result = new ArrayList<>();
		result.add(entity);
		
		String id = ce.getEntityId();
		logger.debug("Reading chunks of entity '{}'", id);
		while (it.hasNext())
		{
			E nextEntity = it.next();
			if (!(nextEntity instanceof CradleEntity))  //Something unexpected instead of next chunk
			{
				bufferedNext = nextEntity;
				break;
			}
			
			CradleEntity nextCe = (CradleEntity)nextEntity;
			if (!nextCe.getEntityId().equals(id))  //Chunk of different entity, i.e. our entity wasn't written completely
			{
				bufferedNext = nextEntity;
				break;
			}
			
			result.add(nextEntity);
			if (nextCe.isLastChunk())
			{
				logger.debug("Entity '{}' read as {} chunk(s)", id, result.size());
				return result;
			}
		}
		
		logger.warn("Entity '{}' is incomplete", id);
		return result;
	}
}
