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

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.BookToAdd;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.util.Collection;

/**
 * Contains information about book as stored in "cradle" keyspace
 */
@Entity
public class BookEntity
{
	public static final String BOOK_NAME_PREFIX = "book_",
			FIELD_NAME = "name",
			FIELD_FULLNAME = "fullname",
			FIELD_KEYSPACE_NAME = "keyspace_name",
			FIELD_DESCRIPTION = "description",
			FIELD_CREATED = "created",
			FIELD_SCHEMA_VERSION = "schema_version";

	@PartitionKey(0)
	@CqlName(FIELD_NAME)
	private String name;
	
	@CqlName(FIELD_FULLNAME)
	private String fullName;
	
	@CqlName(FIELD_KEYSPACE_NAME)
	private String keyspaceName;
	
	@CqlName(FIELD_DESCRIPTION)
	private String desc;
	
	@CqlName(FIELD_CREATED)
	private Instant created;

	@CqlName(FIELD_SCHEMA_VERSION)
	private String schemaVersion;

	public BookEntity()
	{
	}
	
	public BookEntity(BookToAdd book, String schemaVersion)
	{
		this.name = book.getName();
		this.fullName = book.getFullName();
		this.keyspaceName = toKeyspaceName(name);
		this.desc = book.getDesc();
		this.created = book.getCreated();
		this.schemaVersion = schemaVersion;
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


	public String getSchemaVersion() {
		return schemaVersion;
	}

	public void setSchemaVersion(String schemaVersion) {
		this.schemaVersion = schemaVersion;
	}


	public BookInfo toBookInfo(Collection<PageInfo> pages) throws CradleStorageException
	{
		return new BookInfo(new BookId(name), fullName, desc, created, pages);
	}
	
	private String toKeyspaceName(String name)
	{
		// Usually, book name is already checked in addBook() method in CradleStorage and has no invalid characters.
		// It's enough to convert name to lower case and add prefix.
		return BOOK_NAME_PREFIX.concat(name.toLowerCase());
	}
}
