/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
