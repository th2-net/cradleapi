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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.StoredTestEvent;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.TestEventUtils;

public class TestEventsIteratorAdapter implements Iterable<StoredTestEvent>
{
	private final ResultSet rs;
	
	public TestEventsIteratorAdapter(String reportId, QueryExecutor exec, String keyspace, String testEventsTable, UUID instanceId) throws IOException
	{
		Select selectFrom = TestEventUtils.prepareSelect(keyspace, testEventsTable, instanceId)
				.whereColumn(REPORT_ID).isEqualTo(literal(reportId));
		
		this.rs = exec.executeQuery(selectFrom.asCql());
	}
	
	@Override
	public Iterator<StoredTestEvent> iterator()
	{
		return new TestEventsIterator(rs.iterator());
	}
}
