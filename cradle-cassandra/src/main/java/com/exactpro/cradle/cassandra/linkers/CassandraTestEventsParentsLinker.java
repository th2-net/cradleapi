/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.linkers;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsParentsLinker;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CassandraTestEventsParentsLinker implements TestEventsParentsLinker
{
	private final QueryExecutor exec;
	private final String keyspace,
			linksTable;
	private final UUID instanceId;
	
	public CassandraTestEventsParentsLinker(QueryExecutor exec, String keyspace, String linksTable, UUID instanceId)
	{
		this.exec = exec;
		this.keyspace = keyspace;
		this.linksTable = linksTable;
		this.instanceId = instanceId;
	}
	
	@Override
	public List<StoredTestEventId> getChildrenIds(StoredTestEventId parentId) throws IOException
	{
		Select selectFrom = prepareQuery(parentId);
		Iterator<Row> it = exec.executeQuery(selectFrom.asCql()).iterator();
		
		List<StoredTestEventId> result = new ArrayList<>();
		while (it.hasNext())
		{
			Row r = it.next();
			result.add(new StoredTestEventId(r.getString(TEST_EVENT_ID)));
		}
		return result;
	}
	
	@Override
	public boolean isTestEventHasChildren(StoredTestEventId parentId) throws IOException
	{
		Select selectFrom = prepareQuery(parentId)
				.limit(1);
		
		return exec.executeQuery(selectFrom.asCql()).one() != null;
	}
	
	
	private Select prepareQuery(StoredTestEventId parentId)
	{
		return selectFrom(keyspace, linksTable)
				.column(TEST_EVENT_ID)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(PARENT_ID).isEqualTo(literal(parentId.toString()));
	}
}