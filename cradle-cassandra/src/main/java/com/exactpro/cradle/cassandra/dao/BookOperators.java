/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
import com.exactpro.cradle.cassandra.dao.books.PageNameEntity;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageSession;
import com.exactpro.cradle.cassandra.dao.books.PageNameOperator;
import com.exactpro.cradle.cassandra.dao.books.PageOperator;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedSession;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalEntity;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.messages.converters.GroupedMessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.dao.messages.converters.MessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.dao.messages.converters.PageSessionEntityConverter;
import com.exactpro.cradle.cassandra.dao.messages.converters.SessionEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.dao.testevents.converters.PageScopeEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.ScopeEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;
import com.exactpro.cradle.cassandra.utils.LimitedCache;

public class BookOperators
{
	private final BookId bookId;

	private final PageOperator pageOperator;
	private final PageNameOperator pageNameOperator;
	
	private final SessionsOperator sessionsOperator;
	private final ScopeOperator scopeOperator;

	private final MessageBatchOperator messageBatchOperator;
	private final GroupedMessageBatchOperator groupedMessageBatchOperator;
	private final PageSessionsOperator pageSessionsOperator;
	private final TestEventOperator testEventOperator;
	private final PageScopesOperator pageScopesOperator;
	private final IntervalOperator intervalOperator;
	private final MessageStatisticsOperator messageStatisticsOperator;
	private final EntityStatisticsOperator entityStatisticsOperator;
	private final SessionStatisticsOperator sessionStatisticsOperator;

	private final SessionEntityConverter sessionEntityConverter;
	private final ScopeEntityConverter scopeEntityConverter;
	private final MessageBatchEntityConverter messageBatchEntityConverter;
	private final GroupedMessageBatchEntityConverter groupedMessageBatchEntityConverter;
	private final PageSessionEntityConverter pageSessionEntityConverter;
	private final TestEventEntityConverter testEventEntityConverter;
	private final PageScopeEntityConverter pageScopeEntityConverter;
	private final MessageStatisticsEntityConverter messageStatisticsEntityConverter;
	private final EntityStatisticsEntityConverter entityStatisticsEntityConverter;
	private final SessionStatisticsEntityConverter sessionStatisticsEntityConverter;

	private final LimitedCache<CachedSession> sessionsCache;
	private final LimitedCache<CachedPageSession> pageSessionsCache;
	private final LimitedCache<CachedScope> scopesCache;
	private final LimitedCache<CachedPageScope> pageScopesCache;
	
	public BookOperators(BookId bookId, CassandraDataMapper dataMapper, String keyspace, CassandraStorageSettings settings)
	{
		this.bookId = bookId;

		pageOperator = dataMapper.pageOperator(keyspace, PageEntity.TABLE_NAME);
		pageNameOperator = dataMapper.pageNameOperator(keyspace, PageNameEntity.TABLE_NAME);
		
		sessionsOperator = dataMapper.sessionsOperator(keyspace, SessionEntity.TABLE_NAME);
		scopeOperator = dataMapper.scopeOperator(keyspace, ScopeEntity.TABLE_NAME);
		
		messageBatchOperator = dataMapper.messageBatchOperator(keyspace, MessageBatchEntity.TABLE_NAME);
		groupedMessageBatchOperator = dataMapper.groupedMessageBatchOperator(keyspace, settings.getGroupedMessagesTable());
		pageSessionsOperator = dataMapper.pageSessionsOperator(keyspace, PageSessionEntity.TABLE_NAME);
		testEventOperator = dataMapper.testEventOperator(keyspace, settings.getTestEventsTable());
		pageScopesOperator = dataMapper.pageScopesOperator(keyspace, PageScopeEntity.TABLE_NAME);
		messageStatisticsOperator = dataMapper.messageStatisticsOperator(keyspace, MessageStatisticsEntity.TABLE_NAME);
		entityStatisticsOperator = dataMapper.entityStatisticsOperator(keyspace, EntityStatisticsEntity.TABLE_NAME);
		sessionStatisticsOperator = dataMapper.sessionStatisticsOperator(keyspace, SessionStatisticsEntity.TABLE_NAME);

		intervalOperator = dataMapper.intervalOperator(keyspace, IntervalEntity.TABLE_NAME);

		sessionEntityConverter = dataMapper.sessionEntityConverter();
		scopeEntityConverter = dataMapper.scopeEntityConverter();
		messageBatchEntityConverter = dataMapper.messageBatchEntityConverter();
		groupedMessageBatchEntityConverter = dataMapper.groupedMessageBatchEntityConverter();
		pageSessionEntityConverter = dataMapper.pageSessionEntityConverter();
		testEventEntityConverter = dataMapper.testEventEntityConverter();
		pageScopeEntityConverter = dataMapper.pageScopeEntityConverter();
		messageStatisticsEntityConverter = dataMapper.messageStatisticsEntityConverter();
		entityStatisticsEntityConverter = dataMapper.entityStatisticsEntityConverter();
		sessionStatisticsEntityConverter = dataMapper.sessionStatisticsEntityConverter();

		sessionsCache = new LimitedCache<>(settings.getSessionsCacheSize());
		pageSessionsCache = new LimitedCache<>(settings.getPageSessionsCacheSize());
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
	
	public SessionsOperator getSessionsOperator()
	{
		return sessionsOperator;
	}
	
	public ScopeOperator getScopeOperator()
	{
		return scopeOperator;
	}

	public MessageBatchOperator getMessageBatchOperator()
	{
		return messageBatchOperator;
	}

	public GroupedMessageBatchOperator getGroupedMessageBatchOperator()
	{
		return groupedMessageBatchOperator;
	}

	public PageSessionsOperator getPageSessionsOperator()
	{
		return pageSessionsOperator;
	}

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

	public MessageStatisticsOperator getMessageStatisticsOperator() {
		return messageStatisticsOperator;
	}

	public EntityStatisticsOperator getEntityStatisticsOperator() {
		return entityStatisticsOperator;
	}

	public SessionStatisticsOperator getSessionStatisticsOperator() {
		return sessionStatisticsOperator;
	}

	public SessionEntityConverter getSessionEntityConverter()
	{
		return sessionEntityConverter;
	}

	public ScopeEntityConverter getScopeEntityConverter()
	{
		return scopeEntityConverter;
	}

	public MessageBatchEntityConverter getMessageBatchEntityConverter()
	{
		return messageBatchEntityConverter;
	}

	public GroupedMessageBatchEntityConverter getGroupedMessageBatchEntityConverter()
	{
		return groupedMessageBatchEntityConverter;
	}

	public PageSessionEntityConverter getPageSessionEntityConverter()
	{
		return pageSessionEntityConverter;
	}

	public TestEventEntityConverter getTestEventEntityConverter()
	{
		return testEventEntityConverter;
	}

	public PageScopeEntityConverter getPageScopeEntityConverter()
	{
		return pageScopeEntityConverter;
	}

	public MessageStatisticsEntityConverter getMessageStatisticsEntityConverter() {
		return messageStatisticsEntityConverter;
	}

	public EntityStatisticsEntityConverter getEntityStatisticsEntityConverter() {
		return entityStatisticsEntityConverter;
	}

	public SessionStatisticsEntityConverter getSessionStatisticsEntityConverter() {
		return sessionStatisticsEntityConverter;
	}

	public LimitedCache<CachedSession> getSessionsCache()
	{
		return sessionsCache;
	}
	
	public LimitedCache<CachedPageSession> getPageSessionsCache()
	{
		return pageSessionsCache;
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
