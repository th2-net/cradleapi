/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.retries;

import com.datastax.oss.driver.api.core.CqlSession;

public class PagingSupplies
{
	private final CqlSession session;
	private final SelectExecutionPolicy execPolicy;
	private final int maxPageSize;
	
	public PagingSupplies(CqlSession session, SelectExecutionPolicy execPolicy, int maxPageSize)
	{
		this.session = session;
		this.execPolicy = execPolicy;
		this.maxPageSize = maxPageSize;
	}
	
	
	public CqlSession getSession()
	{
		return session;
	}
	
	public SelectExecutionPolicy getExecPolicy()
	{
		return execPolicy;
	}
	
	public int getMaxPageSize()
	{
		return maxPageSize;
	}
}
