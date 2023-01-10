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

import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.books.*;
import com.exactpro.cradle.cassandra.dao.books.converters.PageEntityConverter;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageSession;
import com.exactpro.cradle.cassandra.dao.cache.CachedScope;
import com.exactpro.cradle.cassandra.dao.cache.CachedSession;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalEntity;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.intervals.converters.IntervalEntityConverter;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.messages.converters.*;
import com.exactpro.cradle.cassandra.dao.testevents.*;
import com.exactpro.cradle.cassandra.dao.testevents.converters.PageScopeEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.ScopeEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;
import com.exactpro.cradle.cassandra.utils.LimitedCache;

public class CassandraOperators {
    private final BookOperator bookOperator;
    private final PageOperator pageOperator;
    private final PageNameOperator pageNameOperator;

    private final SessionsOperator sessionsOperator;
    private final ScopeOperator scopeOperator;

    private final MessageBatchOperator messageBatchOperator;
    private final GroupedMessageBatchOperator groupedMessageBatchOperator;
    private final PageSessionsOperator pageSessionsOperator;
    private final GroupsOperator groupsOperator;
    private final PageGroupsOperator pageGroupsOperator;
    private final TestEventOperator testEventOperator;
    private final PageScopesOperator pageScopesOperator;
    private final IntervalOperator intervalOperator;
    private final MessageStatisticsOperator messageStatisticsOperator;
    private final EntityStatisticsOperator entityStatisticsOperator;
    private final SessionStatisticsOperator sessionStatisticsOperator;
    private final EventBatchMaxDurationOperator eventBatchMaxDurationOperator;

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
    private final GroupEntityConverter groupEntityConverter;
    private final PageGroupEntityConverter pageGroupEntityConverter;
    private final IntervalEntityConverter intervalEntityConverter;
    private final PageEntityConverter pageEntityConverter;

    private final LimitedCache<CachedSession> sessionsCache;
    private final LimitedCache<CachedPageSession> pageSessionsCache;
    private final LimitedCache<CachedScope> scopesCache;
    private final LimitedCache<CachedPageScope> pageScopesCache;
    private final LimitedCache<GroupEntity> groupCache;
    private final LimitedCache<PageGroupEntity> pageGroupCache;
    private final LimitedCache<SessionStatisticsEntity> sessionStatisticsCache;

    public CassandraOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings) {

        String keyspace = settings.getKeyspace();
        bookOperator = dataMapper.cradleBookOperator(keyspace, BookEntity.TABLE_NAME);
        pageOperator = dataMapper.pageOperator(keyspace, PageEntity.TABLE_NAME);
        pageNameOperator = dataMapper.pageNameOperator(keyspace, PageNameEntity.TABLE_NAME);

        sessionsOperator = dataMapper.sessionsOperator(keyspace, SessionEntity.TABLE_NAME);
        scopeOperator = dataMapper.scopeOperator(keyspace, ScopeEntity.TABLE_NAME);

        messageBatchOperator = dataMapper.messageBatchOperator(keyspace, MessageBatchEntity.TABLE_NAME);
        groupedMessageBatchOperator = dataMapper.groupedMessageBatchOperator(keyspace, GroupedMessageBatchEntity.TABLE_NAME);
        pageSessionsOperator = dataMapper.pageSessionsOperator(keyspace, PageSessionEntity.TABLE_NAME);
        testEventOperator = dataMapper.testEventOperator(keyspace, TestEventEntity.TABLE_NAME);
        pageScopesOperator = dataMapper.pageScopesOperator(keyspace, PageScopeEntity.TABLE_NAME);
        groupsOperator = dataMapper.groupsOperator(keyspace, GroupEntity.TABLE_NAME);
        pageGroupsOperator = dataMapper.pageGroupsOperator(keyspace, PageGroupEntity.TABLE_NAME);
        messageStatisticsOperator = dataMapper.messageStatisticsOperator(keyspace, MessageStatisticsEntity.TABLE_NAME);
        entityStatisticsOperator = dataMapper.entityStatisticsOperator(keyspace, EntityStatisticsEntity.TABLE_NAME);
        sessionStatisticsOperator = dataMapper.sessionStatisticsOperator(keyspace, SessionStatisticsEntity.TABLE_NAME);
        eventBatchMaxDurationOperator = dataMapper.eventBatchMaxDurationOperator(keyspace, EventBatchMaxDurationEntity.TABLE_NAME);

        intervalOperator = dataMapper.intervalOperator(keyspace, IntervalEntity.TABLE_NAME);

        sessionEntityConverter = dataMapper.sessionEntityConverter();
        scopeEntityConverter = dataMapper.scopeEntityConverter();
        messageBatchEntityConverter = dataMapper.messageBatchEntityConverter();
        groupedMessageBatchEntityConverter = dataMapper.groupedMessageBatchEntityConverter();
        pageSessionEntityConverter = dataMapper.pageSessionEntityConverter();
        testEventEntityConverter = dataMapper.testEventEntityConverter();
        pageScopeEntityConverter = dataMapper.pageScopeEntityConverter();
        groupEntityConverter = dataMapper.groupEntityConverter();
        pageGroupEntityConverter = dataMapper.pageGroupEntityConverter();
        messageStatisticsEntityConverter = dataMapper.messageStatisticsEntityConverter();
        entityStatisticsEntityConverter = dataMapper.entityStatisticsEntityConverter();
        sessionStatisticsEntityConverter = dataMapper.sessionStatisticsEntityConverter();
        intervalEntityConverter = dataMapper.intervalEntityConverter();
        pageEntityConverter = dataMapper.pageEntityConverter();

        sessionsCache = new LimitedCache<>(settings.getSessionsCacheSize());
        pageSessionsCache = new LimitedCache<>(settings.getPageSessionsCacheSize());
        groupCache = new LimitedCache<>(settings.getGroupsCacheSize());
        pageGroupCache = new LimitedCache<>(settings.getPageGroupsCacheSize());
        scopesCache = new LimitedCache<>(settings.getScopesCacheSize());
        pageScopesCache = new LimitedCache<>(settings.getPageScopesCacheSize());
        sessionStatisticsCache = new LimitedCache<>(settings.getSessionStatisticsCacheSize());
    }

    public BookOperator getBookOperator() {
        return this.bookOperator;
    }

    public PageOperator getPageOperator() {
        return pageOperator;
    }

    public PageNameOperator getPageNameOperator() {
        return pageNameOperator;
    }

    public SessionsOperator getSessionsOperator() {
        return sessionsOperator;
    }

    public ScopeOperator getScopeOperator() {
        return scopeOperator;
    }

    public MessageBatchOperator getMessageBatchOperator() {
        return messageBatchOperator;
    }

    public GroupedMessageBatchOperator getGroupedMessageBatchOperator() {
        return groupedMessageBatchOperator;
    }

    public PageSessionsOperator getPageSessionsOperator() {
        return pageSessionsOperator;
    }

    public TestEventOperator getTestEventOperator() {
        return testEventOperator;
    }

    public PageScopesOperator getPageScopesOperator() {
        return pageScopesOperator;
    }

    public IntervalOperator getIntervalOperator() {
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

    public EventBatchMaxDurationOperator getEventBatchMaxDurationOperator() {
        return eventBatchMaxDurationOperator;
    }

    public SessionEntityConverter getSessionEntityConverter() {
        return sessionEntityConverter;
    }

    public ScopeEntityConverter getScopeEntityConverter() {
        return scopeEntityConverter;
    }

    public MessageBatchEntityConverter getMessageBatchEntityConverter() {
        return messageBatchEntityConverter;
    }

    public GroupedMessageBatchEntityConverter getGroupedMessageBatchEntityConverter() {
        return groupedMessageBatchEntityConverter;
    }

    public PageSessionEntityConverter getPageSessionEntityConverter() {
        return pageSessionEntityConverter;
    }

    public TestEventEntityConverter getTestEventEntityConverter() {
        return testEventEntityConverter;
    }

    public PageScopeEntityConverter getPageScopeEntityConverter() {
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

    public LimitedCache<CachedSession> getSessionsCache() {
        return sessionsCache;
    }

    public LimitedCache<CachedPageSession> getPageSessionsCache() {
        return pageSessionsCache;
    }

    public LimitedCache<CachedScope> getScopesCache() {
        return scopesCache;
    }

    public LimitedCache<CachedPageScope> getPageScopesCache() {
        return pageScopesCache;
    }

    public LimitedCache<SessionStatisticsEntity> getSessionStatisticsCache() {
        return sessionStatisticsCache;
    }

    public GroupsOperator getGroupsOperator() {
        return groupsOperator;
    }

    public PageGroupsOperator getPageGroupsOperator() {
        return pageGroupsOperator;
    }

    public GroupEntityConverter getGroupEntityConverter() {
        return groupEntityConverter;
    }

    public PageGroupEntityConverter getPageGroupEntityConverter() {
        return pageGroupEntityConverter;
    }

    public IntervalEntityConverter getIntervalEntityConverter() {
        return intervalEntityConverter;
    }

    public PageEntityConverter getPageEntityConverter() {
        return pageEntityConverter;
    }

    public LimitedCache<GroupEntity> getGroupCache() {
        return groupCache;
    }

    public LimitedCache<PageGroupEntity> getPageGroupCache() {
        return pageGroupCache;
    }
}
