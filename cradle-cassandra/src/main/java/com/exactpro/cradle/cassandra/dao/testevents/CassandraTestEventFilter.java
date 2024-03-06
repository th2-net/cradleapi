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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.testevents.StoredTestEventId;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.FIELD_BOOK;
import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.FIELD_ID;
import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.FIELD_PAGE;
import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.FIELD_PARENT_ID;
import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.FIELD_SCOPE;
import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.FIELD_START_DATE;
import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.FIELD_START_TIME;
import static java.util.Objects.requireNonNull;

public class CassandraTestEventFilter implements CassandraFilter<TestEventEntity> {
    private static final String START_DATE_FROM = "startDateFrom";
    private static final String START_DATE_TO = "startDateTo";
    private static final String START_TIME_FROM = "startTimeFrom";
    private static final String START_TIME_TO = "startTimeTo";
    private static final String ID = "id";

    private final @Nonnull String scope;
    private final @Nonnull PageId pageId;
    private final FilterForGreater<Instant> startTimestampFrom;
    private final FilterForLess<Instant> startTimestampTo;
    private final String parentId;
    private final StoredTestEventId id;

    /** limit must be strictly positive ( limit greater than 0 ) */
    private final int limit;
    private final Order order;

    public CassandraTestEventFilter(PageId pageId, String scope,
                                    FilterForGreater<Instant> startTimestampFrom, FilterForLess<Instant> startTimestampTo,
                                    StoredTestEventId id,
                                    String parentId, int limit, Order order) {
        this.pageId = requireNonNull(pageId, "page id can't be null because book and page names are part of partition");
        this.scope = requireNonNull(scope, "scope can't be null because it is part of partition");
        this.startTimestampFrom = startTimestampFrom;
        this.startTimestampTo = startTimestampTo;
        this.id = id;
        this.parentId = parentId;
        this.limit = limit;
        this.order = (order == null) ? Order.DIRECT : order;
    }


    @Override
    public Select addConditions(Select select) {
        select = select
                .whereColumn(FIELD_BOOK).isEqualTo(bindMarker())
                .whereColumn(FIELD_PAGE).isEqualTo(bindMarker())
                .whereColumn(FIELD_SCOPE).isEqualTo(bindMarker());

        if (startTimestampFrom != null)
            select = FilterUtils.timestampFilterToWhere(startTimestampFrom.getOperation(), select, FIELD_START_DATE, FIELD_START_TIME, START_DATE_FROM, START_TIME_FROM);

        if (startTimestampTo != null)
            select = FilterUtils.timestampFilterToWhere(startTimestampTo.getOperation(), select, FIELD_START_DATE, FIELD_START_TIME, START_DATE_TO, START_TIME_TO);

        if (id != null) {
            if (order == Order.DIRECT)
                select = FilterUtils.timestampAndIdFilterToWhere(ComparisonOperation.GREATER_OR_EQUALS, select, FIELD_START_DATE, FIELD_START_TIME, FIELD_ID, START_DATE_FROM, START_TIME_FROM, ID);
            else
                select = FilterUtils.timestampAndIdFilterToWhere(ComparisonOperation.LESS_OR_EQUALS, select, FIELD_START_DATE, FIELD_START_TIME, FIELD_ID, START_DATE_TO, START_TIME_TO, ID);
        }

        if (parentId != null)
            select = select.whereColumn(FIELD_PARENT_ID).isEqualTo(bindMarker());
        else {
            // ordering is not supported when filtering by parent id requested
            ClusteringOrder orderBy = (order == Order.DIRECT) ? ClusteringOrder.ASC : ClusteringOrder.DESC;
            select = select
                    .orderBy(FIELD_START_DATE, orderBy)
                    .orderBy(FIELD_START_TIME, orderBy)
                    .orderBy(FIELD_ID, orderBy);
        }

        if (limit > 0) {
            select = select.limit(limit);
        }

        return select;
    }


    @Override
    public BoundStatementBuilder bindParameters(BoundStatementBuilder builder) {
        builder = builder
                .setString(FIELD_BOOK, pageId.getBookId().getName())
                .setString(FIELD_PAGE, pageId.getName())
                .setString(FIELD_SCOPE, scope);

        if (startTimestampFrom != null)
            builder = FilterUtils.bindTimestamp(startTimestampFrom.getValue(), builder, START_DATE_FROM, START_TIME_FROM);

        if (startTimestampTo != null)
            builder = FilterUtils.bindTimestamp(startTimestampTo.getValue(), builder, START_DATE_TO, START_TIME_TO);

        if (id != null) {
            if (order == Order.DIRECT)
                builder = FilterUtils.bindTimestampAndId(id.getStartTimestamp(), id.getId(), builder, START_DATE_FROM, START_TIME_FROM, ID);
            else
                builder = FilterUtils.bindTimestampAndId(id.getStartTimestamp(), id.getId(), builder, START_DATE_TO, START_TIME_TO, ID);
        }

        if (parentId != null)
            builder = builder.setString(FIELD_PARENT_ID, parentId);

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

    public @Nonnull String getScope() {
        return scope;
    }

    public StoredTestEventId getId() {
        return id;
    }

    public FilterForGreater<Instant> getStartTimestampFrom() {
        return startTimestampFrom;
    }

    public FilterForLess<Instant> getStartTimestampTo() {
        return startTimestampTo;
    }

    public String getParentId() {
        return parentId;
    }

    public Order getOrder() {
        return order;
    }

    @Override
    public String toString() {
        List<String> result = new ArrayList<>(10);
        result.add("pageId=" + pageId);
        result.add("scope=" + scope);
        if (startTimestampFrom != null)
            result.add("timestampFrom" + startTimestampFrom);
        if (startTimestampTo != null)
            result.add("timestampTo" + startTimestampTo);
        if (id != null)
            result.add("id=" + id);
        if (parentId != null)
            result.add("parentId=" + parentId);
        if (order != null)
            result.add("order=" + order);
        return String.join(", ", result);
    }
}
