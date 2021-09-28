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

import static com.exactpro.cradle.cassandra.StorageConstants.BOOK;
import static com.exactpro.cradle.cassandra.StorageConstants.SCOPE;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

@Entity
public class ScopeEntity
{
	@PartitionKey(0)
	@CqlName(BOOK)
	private String book;
	
	@ClusteringColumn(0)
	@CqlName(SCOPE)
	private String scope;
	
	public ScopeEntity()
	{
	}
	
	public ScopeEntity(String book, String scope)
	{
		this.book = book;
		this.scope = scope;
	}
	
	
	public String getBook()
	{
		return book;
	}
	
	public void setBook(String book)
	{
		this.book = book;
	}
	
	
	public String getScope()
	{
		return scope;
	}
	
	public void setScope(String scope)
	{
		this.scope = scope;
	}
}
