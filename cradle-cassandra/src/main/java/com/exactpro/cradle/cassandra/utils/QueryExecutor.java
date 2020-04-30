/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.utils;

import java.io.IOException;
import java.time.Duration;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class QueryExecutor
{
	private final CqlSession session;
	private final long timeout;
	private final ConsistencyLevel writeConsistencyLevel,
			readConsistencyLevel;
	
	public QueryExecutor(CqlSession session, long timeout, ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
	{
		this.session = session;
		this.timeout = timeout;
		this.writeConsistencyLevel = writeConsistencyLevel;
		this.readConsistencyLevel = readConsistencyLevel;
	}
	
	public ResultSet executeQuery(String cqlQuery, boolean writingQuery) throws IOException
	{
		ResultSet rs = session.execute(makeSimpleStatement(cqlQuery, 
				writingQuery ? writeConsistencyLevel : readConsistencyLevel));
		if (!rs.wasApplied())
			throw new IOException("Query was rejected by database. Probably, key fields are duplicated. Rejected query: "+cqlQuery);
		return rs;
	}
	
	public CqlSession getSession()
	{
		return session;
	}
	
	
	private SimpleStatement makeSimpleStatement(String query, ConsistencyLevel consistencyLevel)
	{
		return SimpleStatement.newInstance(query)
				.setTimeout(Duration.ofMillis(timeout))
				.setConsistencyLevel(consistencyLevel);
	}
}