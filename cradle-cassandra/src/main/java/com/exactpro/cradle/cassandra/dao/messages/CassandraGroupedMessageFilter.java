/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.StringJoiner;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.dao.messages.CassandraStoredMessageFilter.DATE_FROM;
import static com.exactpro.cradle.cassandra.dao.messages.CassandraStoredMessageFilter.DATE_TO;
import static com.exactpro.cradle.cassandra.dao.messages.CassandraStoredMessageFilter.TIME_FROM;
import static com.exactpro.cradle.cassandra.dao.messages.CassandraStoredMessageFilter.TIME_TO;
import static com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity.FIELD_ALIAS_GROUP;
import static com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity.FIELD_BOOK;
import static com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity.FIELD_FIRST_MESSAGE_DATE;
import static com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity.FIELD_FIRST_MESSAGE_TIME;
import static com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity.FIELD_PAGE;
import static java.util.Objects.requireNonNull;

public class CassandraGroupedMessageFilter implements CassandraFilter<GroupedMessageBatchEntity> {
    private final @Nonnull String groupName;
    private final @Nonnull PageId pageId;

    /** limit must be strictly positive ( limit greater than 0 ) */
    private final int limit;
    private final FilterForGreater<Instant> messageTimeFrom;
    private final FilterForLess<Instant> messageTimeTo;
    private final Order order;

    public CassandraGroupedMessageFilter(PageId pageId,
                                         String groupName,
                                         FilterForGreater<Instant> messageTimeFrom,
                                         FilterForLess<Instant> messageTimeTo,
                                         Order order,
                                         int limit) {
        this.pageId = requireNonNull(pageId, "page id can't be null because book and page names are part of partition");
        this.groupName = requireNonNull(groupName, "group name can't be null because it is part of partition");
        this.messageTimeFrom = messageTimeFrom;
        this.messageTimeTo = messageTimeTo;
        this.order = order;
        this.limit = limit;
    }

    @Override
    public Select addConditions(Select select) {
        select = select
                .whereColumn(FIELD_BOOK).isEqualTo(bindMarker())
                .whereColumn(FIELD_PAGE).isEqualTo(bindMarker())
                .whereColumn(FIELD_ALIAS_GROUP).isEqualTo(bindMarker());

        if (messageTimeFrom != null)
            select = FilterUtils.timestampFilterToWhere(messageTimeFrom.getOperation(), select, FIELD_FIRST_MESSAGE_DATE, FIELD_FIRST_MESSAGE_TIME, DATE_FROM, TIME_FROM);
        if (messageTimeTo != null)
            select = FilterUtils.timestampFilterToWhere(messageTimeTo.getOperation(), select, FIELD_FIRST_MESSAGE_DATE, FIELD_FIRST_MESSAGE_TIME, DATE_TO, TIME_TO);

        if (limit > 0) {
            select = select.limit(limit);
        }

        if (this.order == null || this.order == Order.DIRECT) {
            select = select.orderBy(FIELD_FIRST_MESSAGE_DATE, ClusteringOrder.ASC)
                    .orderBy(FIELD_FIRST_MESSAGE_TIME, ClusteringOrder.ASC);
        } else {
            select = select.orderBy(FIELD_FIRST_MESSAGE_DATE, ClusteringOrder.DESC)
                    .orderBy(FIELD_FIRST_MESSAGE_TIME, ClusteringOrder.DESC);
        }

        return select;
    }

    @Override
    public BoundStatementBuilder bindParameters(BoundStatementBuilder builder) {
        builder = builder
                .setString(FIELD_BOOK, pageId.getBookId().getName())
                .setString(FIELD_PAGE, pageId.getName())
                .setString(FIELD_ALIAS_GROUP, groupName);

        if (messageTimeFrom != null)
            builder = FilterUtils.bindTimestamp(messageTimeFrom.getValue(), builder, DATE_FROM, TIME_FROM);
        if (messageTimeTo != null)
            builder = FilterUtils.bindTimestamp(messageTimeTo.getValue(), builder, DATE_TO, TIME_TO);

        return builder;
    }

    public @Nonnull PageId getPageId() {
        return pageId;
    }

    public @Nonnull String getBook() {
        return pageId.getBookId().getName();
    }

    public @Nonnull String getPage() {
        return pageId.getName();
    }

    public @Nonnull String getGroupName() {
        return groupName;
    }

    public int getLimit() {
        return limit;
    }

    public FilterForGreater<Instant> getMessageTimeFrom() {
        return messageTimeFrom;
    }

    public FilterForLess<Instant> getMessageTimeTo() {
        return messageTimeTo;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CassandraGroupedMessageFilter.class.getSimpleName() + "[", "]")
                .add("pageId='" + pageId + "'")
                .add("groupName='" + groupName + "'")
                .add("limit=" + limit)
                .add("messageTimeFrom " + messageTimeFrom)
                .add("messageTimeTo " + messageTimeTo)
                .add("order=" + order)
                .toString();
    }
}
