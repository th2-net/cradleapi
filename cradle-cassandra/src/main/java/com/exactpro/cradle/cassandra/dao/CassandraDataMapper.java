/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.mapper.annotations.*;
import com.exactpro.cradle.cassandra.dao.books.BookOperator;
import com.exactpro.cradle.cassandra.dao.books.PageNameOperator;
import com.exactpro.cradle.cassandra.dao.books.PageOperator;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalOperator;
import com.exactpro.cradle.cassandra.dao.intervals.converters.IntervalEntityConverter;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.messages.converters.*;
import com.exactpro.cradle.cassandra.dao.testevents.EventBatchMaxDurationOperator;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeOperator;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.converters.PageScopeEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.ScopeEntityConverter;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventEntityConverter;

@Mapper
public interface CassandraDataMapper {
    @DaoFactory
    BookOperator cradleBookOperator(@DaoKeyspace String keyspace, @DaoTable String booksTable);

    @DaoFactory
    PageOperator pageOperator(@DaoKeyspace String keyspace, @DaoTable String pagesTableName);

    @DaoFactory
    PageNameOperator pageNameOperator(@DaoKeyspace String keyspace, @DaoTable String pageNamesTable);

    @DaoFactory
    SessionsOperator sessionsOperator(@DaoKeyspace String keyspace, @DaoTable String sessionsTable);

    @DaoFactory
    ScopeOperator scopeOperator(@DaoKeyspace String keyspace, @DaoTable String scopesTable);


    @DaoFactory
    MessageBatchOperator messageBatchOperator(@DaoKeyspace String keyspace, @DaoTable String messagesTable);

    @DaoFactory
    GroupedMessageBatchOperator groupedMessageBatchOperator(@DaoKeyspace String keyspace, @DaoTable String groupedMessagesTable);

    @DaoFactory
    PageSessionsOperator pageSessionsOperator(@DaoKeyspace String keyspace, @DaoTable String pageSessionsTable);


    @DaoFactory
    TestEventOperator testEventOperator(@DaoKeyspace String keyspace, @DaoTable String testEventsTable);

    @DaoFactory
    PageScopesOperator pageScopesOperator(@DaoKeyspace String keyspace, @DaoTable String pageScopesTable);

    @DaoFactory
    PageGroupsOperator pageGroupsOperator (@DaoKeyspace String keyspace, @DaoTable String pageGroupsTable);

    @DaoFactory
    GroupsOperator groupsOperator (@DaoKeyspace String keyspace, @DaoTable String groupsTable);

    @DaoFactory
    IntervalOperator intervalOperator(@DaoKeyspace String keyspace, @DaoTable String intervalsTable);

    @DaoFactory
    MessageStatisticsOperator messageStatisticsOperator(@DaoKeyspace String keyspace, @DaoTable String statisticsTable);

    @DaoFactory
    EntityStatisticsOperator entityStatisticsOperator(@DaoKeyspace String keyspace, @DaoTable String statisticsTable);

    @DaoFactory
    SessionStatisticsOperator sessionStatisticsOperator(@DaoKeyspace String keyspace, @DaoTable String statisticsTable);

    @DaoFactory
    EventBatchMaxDurationOperator eventBatchMaxDurationOperator(@DaoKeyspace String keyspace, @DaoTable String statisticsTable);

    @DaoFactory
    SessionEntityConverter sessionEntityConverter();

    @DaoFactory
    ScopeEntityConverter scopeEntityConverter();

    @DaoFactory
    MessageBatchEntityConverter messageBatchEntityConverter();

    @DaoFactory
    GroupedMessageBatchEntityConverter groupedMessageBatchEntityConverter();

    @DaoFactory
    TestEventEntityConverter testEventEntityConverter();

    @DaoFactory
    PageScopeEntityConverter pageScopeEntityConverter();

    @DaoFactory
    PageSessionEntityConverter pageSessionEntityConverter();

    @DaoFactory
    MessageStatisticsEntityConverter messageStatisticsEntityConverter();

    @DaoFactory
    EntityStatisticsEntityConverter entityStatisticsEntityConverter();

    @DaoFactory
    SessionStatisticsEntityConverter sessionStatisticsEntityConverter();

    @DaoFactory
    PageGroupEntityConverter pageGroupEntityConverter();

    @DaoFactory
    GroupEntityConverter groupEntityConverter();

    @DaoFactory
    IntervalEntityConverter intervalEntityConverter();
}
