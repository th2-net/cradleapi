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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class QueryExecutor
{
	private final CqlSession session;
	private final long timeout;
	
	public QueryExecutor(CqlSession session, long timeout)
	{
		this.session = session;
		this.timeout = timeout;
	}
	
	public ResultSet executeQuery(String cqlQuery) throws IOException
	{
		ResultSet rs = session.execute(makeSimpleStatement(cqlQuery));
		if (!rs.wasApplied())
			throw new IOException("Query was rejected by database. Probably, key fields are duplicated. Rejected query: "+cqlQuery);
		return rs;
	}
	
	public CqlSession getSession()
	{
		return session;
	}
	
	
	private SimpleStatement makeSimpleStatement(String query)
	{
		return SimpleStatement.newInstance(query).setTimeout(Duration.ofMillis(timeout));
	}
}
