/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.cassandra.utils.ReportUtils;

public class CassandraReportsIteratorAdapter implements Iterable<StoredReport>
{
	private final ResultSet rs;
	
	public CassandraReportsIteratorAdapter(QueryExecutor exec, String keyspace, String testEventsTable, UUID instanceId) throws IOException
	{
		Select selectFrom = ReportUtils.prepareSelect(keyspace, testEventsTable, instanceId);
		
		this.rs = exec.executeQuery(selectFrom.asCql());
	}
	
	@Override
	public Iterator<StoredReport> iterator()
	{
		return new CassandraReportsIterator(rs.iterator());
	}
}