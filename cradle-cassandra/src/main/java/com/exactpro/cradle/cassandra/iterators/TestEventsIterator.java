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

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.utils.TestEventException;
import com.exactpro.cradle.cassandra.utils.TestEventUtils;
import com.exactpro.cradle.testevents.StoredTestEvent;

public class TestEventsIterator implements Iterator<StoredTestEvent>
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventsIterator.class);
	
	private final Iterator<Row> rows;
	
	public TestEventsIterator(Iterator<Row> rows)
	{
		this.rows = rows;
	}
	
	@Override
	public boolean hasNext()
	{
		return rows.hasNext();
	}
	
	@Override
	public StoredTestEvent next()
	{
		Row r = rows.next();
		try
		{
			return TestEventUtils.toTestEvent(r);
		}
		catch (TestEventException e)
		{
			StoredTestEvent result = e.getTestEvent();
			logger.warn("Error while getting test event '"+result.getId()+"'. Returned data may be corrupted", e);
			return result;
		}
	}
}