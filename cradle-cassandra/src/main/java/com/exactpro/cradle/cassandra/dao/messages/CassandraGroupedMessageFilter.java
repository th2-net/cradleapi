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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

import java.time.Instant;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity.*;
import static com.exactpro.cradle.cassandra.dao.messages.CassandraStoredMessageFilter.*;

public class CassandraGroupedMessageFilter implements CassandraFilter<GroupedMessageBatchEntity> {
    private final String book, page, groupName;
    private final Integer limit;
    private final FilterForGreater<Instant> messageTimeFrom;
    private final FilterForLess<Instant> messageTimeTo;

    public CassandraGroupedMessageFilter(String book, String page, String groupName,
                                         FilterForGreater<Instant> messageTimeFrom, FilterForLess<Instant> messageTimeTo, int limit) {
        this.book = book;
        this.page = page;
        this.groupName = groupName;
        this.messageTimeFrom = messageTimeFrom;
        this.messageTimeTo = messageTimeTo;
        this.limit = limit;
    }

    public CassandraGroupedMessageFilter(String book, String page, String groupName,
                                         FilterForGreater<Instant> messageTimeFrom, FilterForLess<Instant> messageTimeTo) {
        this(book, page, groupName, messageTimeFrom, messageTimeTo, 0);
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

        if (limit != 0) {
            select.limit(limit);
        }

        return select;
    }

    @Override
    public BoundStatementBuilder bindParameters(BoundStatementBuilder builder) {
        builder = builder
				.setString(FIELD_BOOK, book)
				.setString(FIELD_PAGE, page)
                .setString(FIELD_ALIAS_GROUP, groupName);

        if (messageTimeFrom != null)
            builder = FilterUtils.bindTimestamp(messageTimeFrom.getValue(), builder, DATE_FROM, TIME_FROM);
        if (messageTimeTo != null)
            builder = FilterUtils.bindTimestamp(messageTimeTo.getValue(), builder, DATE_TO, TIME_TO);

        return builder;
    }

	public String getBook() {
		return book;
	}

    public String getPage() {
        return page;
    }

    public String getGroupName() {
        return groupName;
    }

    public Integer getLimit() {
        return limit;
    }

    public FilterForGreater<Instant> getMessageTimeFrom() {
        return messageTimeFrom;
    }

    public FilterForLess<Instant> getMessageTimeTo() {
        return messageTimeTo;
    }
}
