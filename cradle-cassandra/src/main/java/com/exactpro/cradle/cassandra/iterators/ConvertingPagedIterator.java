/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.iterators;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;

/**
 * Wrapper for asynchronous paging iterable that converts retrieved entities into Cradle objects
 * @param <R> - class of objects to get while using the wrapper
 * @param <E> - class of entities obtained from Cassandra
 */
public abstract class ConvertingPagedIterator<R, E> implements Iterator<R>
{
	private static final Logger logger = LoggerFactory.getLogger(ConvertingPagedIterator.class);
	
	private final PagedIterator<E> it;
	
	public ConvertingPagedIterator(MappedAsyncPagingIterable<E> rows)
	{
		this.it = new PagedIterator<>(rows);
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
