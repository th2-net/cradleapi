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

import com.datastax.oss.driver.api.core.PagingIterable;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;

public class TestEventsIteratorAdapter implements Iterable<StoredTestEventWrapper>
{
	private final PagingIterable<TestEventEntity> rows;
	
	public TestEventsIteratorAdapter(PagingIterable<TestEventEntity> rows) throws IOException
	{
		this.rows = rows;
	}
	
	@Override
	public Iterator<StoredTestEventWrapper> iterator()
	{
		return new TestEventsIterator(rows.iterator());
	}
}
