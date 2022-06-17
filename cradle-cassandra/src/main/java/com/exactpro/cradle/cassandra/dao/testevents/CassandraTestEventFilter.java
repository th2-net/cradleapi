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

package com.exactpro.cradle.cassandra.dao.testevents;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import static com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

public class CassandraTestEventFilter implements CassandraFilter<TestEventEntity> {
    private static final String START_DATE_FROM = "startDateFrom";
    private static final String START_DATE_TO = "startDateTo";
    private static final String START_TIME_FROM = "startTimeFrom";
    private static final String START_TIME_TO = "startTimeTo";

    private final String book, page, scope;
    private final FilterForGreater<Instant> startTimestampFrom;
    private final FilterForLess<Instant> startTimestampTo;
    private final String parentId;

    public CassandraTestEventFilter(String book, String page, String scope,
                                    FilterForGreater<Instant> startTimestampFrom, FilterForLess<Instant> startTimestampTo,
                                    String parentId) {
        this.book = book;
        this.page = page;
        this.scope = scope;
        this.startTimestampFrom = startTimestampFrom;
        this.startTimestampTo = startTimestampTo;
        this.parentId = parentId;
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
        if (parentId != null)
            select = select.whereColumn(FIELD_PARENT_ID).isEqualTo(bindMarker());

        return select;
    }

    @Override
    public BoundStatementBuilder bindParameters(BoundStatementBuilder builder) {
        builder = builder
                .setString(FIELD_BOOK, book)
                .setString(FIELD_PAGE, page)
                .setString(FIELD_SCOPE, scope);

        if (startTimestampFrom != null)
            builder = FilterUtils.bindTimestamp(startTimestampFrom.getValue(), builder, START_DATE_FROM, START_TIME_FROM);
        if (startTimestampTo != null)
            builder = FilterUtils.bindTimestamp(startTimestampTo.getValue(), builder, START_DATE_TO, START_TIME_TO);
        if (parentId != null)
            builder = builder.setString(FIELD_PARENT_ID, parentId);

        return builder;
    }


    public String getBook() {
        return book;
    }

    public String getPage() {
        return page;
    }

    public String getScope() {
        return scope;
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


    @Override
    public String toString() {
        List<String> result = new ArrayList<>(10);
        if (book != null)
            result.add("book=" + book);
        if (page != null)
            result.add("page=" + page);
        if (scope != null)
            result.add("scope=" + scope);
        if (startTimestampFrom != null)
            result.add("timestamp" + startTimestampFrom);
        if (startTimestampTo != null)
            result.add("timestamp" + startTimestampTo);
        if (parentId != null)
            result.add("parentId=" + parentId);
        return String.join(", ", result);
    }
}
