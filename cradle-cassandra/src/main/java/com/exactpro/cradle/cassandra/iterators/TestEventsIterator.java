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

import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.utils.CassandraTestEventUtils;
import com.exactpro.cradle.testevents.StoredTestEvent;

public class TestEventsIterator extends EntityIterator<StoredTestEvent>
{
	public TestEventsIterator(Iterator<Row> rows)
	{
		super(rows, "test event");
	}
	
	
	@Override
	protected Collection<StoredTestEvent> rowToCollection(Row row) throws IOException
	{
		return CassandraTestEventUtils.toTestEvents(row);
	}
}