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
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.Row;

public abstract class EntityIterator<T> implements Iterator<T>
{
	private static final Logger logger = LoggerFactory.getLogger(EntityIterator.class);
	
	private final Iterator<Row> rows;
	private final String entityName;
	private Iterator<T> batchIterator;
	
	public EntityIterator(Iterator<Row> rows, String entityName)
	{
		this.rows = rows;
		this.entityName = entityName;
	}
	
	
	protected abstract Collection<T> rowToCollection(Row row) throws IOException;
	
	
	@Override
	public boolean hasNext()
	{
		if (batchIterator != null)
		{
			if (batchIterator.hasNext())
				return true;
			batchIterator = null;
		}
		return rows.hasNext();
	}
	
	@Override
	public T next()
	{
		logger.trace("Getting next {}", entityName);
		
		if (batchIterator != null)
			return batchIterator.next();
		
		Row r = rows.next();
		try
		{
			batchIterator = rowToCollection(r).iterator();
			if (batchIterator.hasNext())
				return batchIterator.next();
			batchIterator = null;
			return null;
		}
		catch (IOException e)
		{
			logger.warn("Error while getting "+entityName, e);
			return null;
		}
	}
}
