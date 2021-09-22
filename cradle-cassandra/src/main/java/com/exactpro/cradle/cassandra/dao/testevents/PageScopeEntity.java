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

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.exactpro.cradle.cassandra.StorageConstants.PAGE;
import static com.exactpro.cradle.cassandra.StorageConstants.SCOPE;
import static com.exactpro.cradle.cassandra.StorageConstants.PART;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

@Entity
public class PageScopeEntity
{
	@PartitionKey(0)
	@CqlName(PAGE)
	private String page;
	
	@ClusteringColumn(0)
	@CqlName(SCOPE)
	private String scope;
	
	@ClusteringColumn(1)
	@CqlName(PART)
	private String part;
	
	public PageScopeEntity()
	{
	}

	public PageScopeEntity(String page, String scope, String part)
	{
		this.page = page;
		this.scope = scope;
		this.part = part;
	}
	
	
	public String getPage()
	{
		return page;
	}
	
	public void setPage(String page)
	{
		this.page = page;
	}
	
	
	public String getScope()
	{
		return scope;
	}
	
	public void setScope(String scope)
	{
		this.scope = scope;
	}
	
	
	public String getPart()
	{
		return part;
	}
	
	public void setPart(String part)
	{
		this.part = part;
	}
}
