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

package com.exactpro.cradle.cassandra.dao;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.books.PageNameOperator;
import com.exactpro.cradle.cassandra.dao.books.PageOperator;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedScope;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeOperator;
//import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.utils.LimitedCache;

public class BookOperators
{
	private final BookId bookId;
	
	private final PageOperator pageOperator;
	private final PageNameOperator pageNameOperator;
	private final ScopeOperator scopeOp;
	
//	private final MessageBatchOperator messageBatchOperator;
	private final TestEventOperator testEventOperator;
	private final PageScopesOperator pageScopesOperator;
	private final IntervalOperator intervalOperator;
	
	private final LimitedCache<CachedScope> scopesCache;
	private final LimitedCache<CachedPageScope> pageScopesCache;
	
	public BookOperators(BookId bookId, CassandraDataMapper dataMapper, String keyspace, CassandraStorageSettings settings)
	{
		this.bookId = bookId;
		
		pageOperator = dataMapper.pageOperator(keyspace, settings.getPagesTable());
		pageNameOperator = dataMapper.pageNameOperator(keyspace, settings.getPagesNamesTable());
		scopeOp = dataMapper.scopeOperator(keyspace, settings.getScopesTable());
//		messageBatchOperator = dataMapper.messageBatchOperator(keyspace, settings.getMessagesTable());
		testEventOperator = dataMapper.testEventOperator(keyspace, settings.getTestEventsTable());
		pageScopesOperator = dataMapper.pageScopesOperator(keyspace, settings.getPageScopesTable());
		intervalOperator = dataMapper.intervalOperator(keyspace, settings.getIntervalsTable());
		
		scopesCache = new LimitedCache<>(settings.getScopesCacheSize());
		pageScopesCache = new LimitedCache<>(settings.getPageScopesCacheSize());
	}
	
	
	public BookId getBookId()
	{
		return bookId;
	}
	
	
	public PageOperator getPageOperator()
	{
		return pageOperator;
	}
	
	public PageNameOperator getPageNameOperator()
	{
		return pageNameOperator;
	}
	
	public ScopeOperator getScopeOperator()
	{
		return scopeOp;
	}
	
	
//	public MessageBatchOperator getMessageBatchOperator()
//	{
//		return messageBatchOperator;
//	}
	
	public TestEventOperator getTestEventOperator()
	{
		return testEventOperator;
	}
	
	public PageScopesOperator getPageScopesOperator()
	{
		return pageScopesOperator;
	}
	
	public IntervalOperator getIntervalOperator()
	{
		return intervalOperator;
	}
	
	
	public LimitedCache<CachedScope> getScopesCache()
	{
		return scopesCache;
	}
	
	public LimitedCache<CachedPageScope> getPageScopesCache()
	{
		return pageScopesCache;
	}
}
