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

package com.exactpro.cradle.cassandra.dao.books;

import static com.exactpro.cradle.cassandra.StorageConstants.CREATED;
import static com.exactpro.cradle.cassandra.StorageConstants.DESCRIPTION;
import static com.exactpro.cradle.cassandra.StorageConstants.FULLNAME;
import static com.exactpro.cradle.cassandra.StorageConstants.KEYSPACE_NAME;
import static com.exactpro.cradle.cassandra.StorageConstants.NAME;

import java.time.Instant;
import java.util.Collection;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageInfo;

/**
 * Contains information about book as stored in "cradle" keyspace
 */
@Entity
public class BookEntity
{
	@PartitionKey(0)
	@CqlName(NAME)
	private String name;
	
	@CqlName(FULLNAME)
	private String fullName;
	
	@CqlName(KEYSPACE_NAME)
	private String keyspaceName;
	
	@CqlName(DESCRIPTION)
	private String desc;
	
	@CqlName(CREATED)
	private Instant created;
	
	public BookEntity()
	{
	}
	
	public BookEntity(BookInfo bookInfo)
	{
		name = bookInfo.getId().getName();
		fullName = bookInfo.getFullName();
		keyspaceName = toKeyspaceName(name);
		desc = bookInfo.getDesc();
		created = bookInfo.getCreated();
	}
	
	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	
	public String getFullName()
	{
		return fullName;
	}
	
	public void setFullName(String fullName)
	{
		this.fullName = fullName;
	}
	
	
	public String getKeyspaceName()
	{
		return keyspaceName;
	}
	
	public void setKeyspaceName(String keyspaceName)
	{
		this.keyspaceName = keyspaceName;
	}
	
	
	public String getDesc()
	{
		return desc;
	}
	
	public void setDesc(String desc)
	{
		this.desc = desc;
	}
	
	
	public Instant getCreated()
	{
		return created;
	}
	
	public void setCreated(Instant created)
	{
		this.created = created;
	}
	
	
	public BookInfo toBookInfo(Collection<PageInfo> pages)
	{
		return new BookInfo(new BookId(name), fullName, desc, created, pages);
	}
	
	
	private String toKeyspaceName(String name)
	{
		return name.toLowerCase().replaceAll("[ \t]", "_");
	}
}
