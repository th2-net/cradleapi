/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.books.CradleBookOperator;
import com.exactpro.cradle.cassandra.dao.cache.CachedSession;
import com.exactpro.cradle.cassandra.dao.messages.SessionsOperator;
import com.exactpro.cradle.cassandra.utils.LimitedCache;
import com.exactpro.cradle.utils.CradleStorageException;

public class CradleOperators
{
	private final Map<BookId, BookOperators> bookOps;
	private final CassandraDataMapper dataMapper;
	private final CassandraStorageSettings settings;
	private final CradleBookOperator cradleBookOp;
	private final SessionsOperator sessionsOperator;

	private final LimitedCache<CachedSession> sessionsCache;

	public CradleOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings)
	{
		bookOps = new ConcurrentHashMap<>();
		this.dataMapper = dataMapper;
		this.settings = settings;
		this.cradleBookOp = dataMapper.cradleBookOperator(settings.getCradleInfoKeyspace(), settings.getBooksTable());
		this.sessionsOperator = dataMapper.sessionsOperator(settings.getCradleInfoKeyspace(), settings.getSessionsTable());

		this.sessionsCache = new LimitedCache<>(settings.getPageSessionsCacheSize());
	}
	
	public BookOperators getOperators(BookId bookId) throws CradleStorageException
	{
		BookOperators result = bookOps.get(bookId);
		if (result == null)
			throw new CradleStorageException("No operators prepared for book '"+bookId+"'");
		return result;
	}
	
	public BookOperators addOperators(BookId bookId, String keyspace)
	{
		BookOperators result = new BookOperators(dataMapper, keyspace, settings);
		bookOps.put(bookId, result);
		return result;
	}
	
	public CradleBookOperator getCradleBookOperator()
	{
		return cradleBookOp;
	}

	public SessionsOperator getSessionsOperator()
	{
		return sessionsOperator;
	}


	public LimitedCache<CachedSession> getSessionsCache()
	{
		return sessionsCache;
	}
}
