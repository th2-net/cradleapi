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

import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;

public class TestEventsIterator implements Iterator<StoredTestEventWrapper>
{
	private final Logger logger = LoggerFactory.getLogger(TestEventsIterator.class);
	
	private final Iterator<TestEventEntity> rows;
	
	public TestEventsIterator(Iterator<TestEventEntity> rows)
	{
		this.rows = rows;
	}
	
	@Override
	public boolean hasNext()
	{
		return rows.hasNext();
	}
	
	@Override
	public StoredTestEventWrapper next()
	{
		logger.trace("Getting next test event");
		
		TestEventEntity r = rows.next();
		try
		{
			return r.toStoredTestEventWrapper();
		}
		catch (IOException e)
		{
			throw new RuntimeException("Error while getting next test event", e);
		}
	}
}