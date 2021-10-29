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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.MultiColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.messages.StoredMessageId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

public class CassandraStoredMessageFilter implements CassandraFilter<MessageBatchEntity>
{
	private static final String DATE_FROM = "dateFrom", DATE_TO = "dateTo",
			TIME_FROM = "timeFrom", TIME_TO = "timeTo";

	private final String page, sessionAlias, direction;

	private final FilterForGreater<Instant> messageTimeFrom;
	private final FilterForLess<Instant> messageTimeTo;
	private final FilterForAny<StoredMessageId> messageId;
	private boolean isTimeFromBounded = false, isTimeToBounded = false;

	public CassandraStoredMessageFilter(String page, String sessionAlias, String direction,
			FilterForGreater<Instant> messageTimeFrom, FilterForLess<Instant> messageTimeTo,
			FilterForAny<StoredMessageId> messageId)
	{
		this.page = page;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		this.messageTimeFrom = messageTimeFrom;
		this.messageTimeTo = messageTimeTo;
		this.messageId = messageId;
	}

	@Override
	public Select addConditions(Select select)
	{
		select = select.whereColumn(PAGE).isEqualTo(bindMarker())
			.whereColumn(SESSION_ALIAS).isEqualTo(bindMarker())
			.whereColumn(DIRECTION).isEqualTo(bindMarker());

		if (messageId != null)
			select = addMessageIdCondition(select);

		if (!isTimeFromBounded && messageTimeFrom != null)
			select = FilterUtils.timestampFilterToWhere(messageTimeFrom.getOperation(), select, MESSAGE_DATE, MESSAGE_TIME, DATE_FROM, TIME_FROM);

		if (!isTimeToBounded && messageTimeTo != null)
			select = FilterUtils.timestampFilterToWhere(messageTimeTo.getOperation(), select, MESSAGE_DATE, MESSAGE_TIME, DATE_TO, TIME_TO);

		return select;
	}

	private Select addMessageIdCondition(Select select)
	{
		MultiColumnRelationBuilder<Select> mcrBuilder = select.whereColumns(MESSAGE_DATE, MESSAGE_TIME, SEQUENCE);
		ComparisonOperation op = messageId.getOperation();
		switch (op)
		{
			case LESS:
			case LESS_OR_EQUALS:
				select = mcrBuilder.isLessThanOrEqualTo(tuple(bindMarker(DATE_TO), bindMarker(TIME_TO), bindMarker(SEQUENCE)));
				isTimeToBounded = true;
				break;
			default:
				select = mcrBuilder.isGreaterThanOrEqualTo(tuple(bindMarker(DATE_FROM), bindMarker(TIME_FROM), bindMarker(SEQUENCE)));
				isTimeFromBounded = true;
		}
		return select;
	}

	@Override
	public BoundStatementBuilder bindParameters(BoundStatementBuilder builder)
	{
		builder = builder.setString(PAGE, page)
				.setString(SESSION_ALIAS, sessionAlias)
				.setString(DIRECTION, direction);

		if (messageId != null)
		{
			builder = builder.setLong(SEQUENCE, messageId.getValue().getSequence());
		}

		if (messageTimeFrom != null)
			builder = FilterUtils.bindTimestamp(messageTimeFrom.getValue(), builder, DATE_FROM, TIME_FROM);

		if (messageTimeTo != null)
			builder = FilterUtils.bindTimestamp(messageTimeTo.getValue(), builder, DATE_TO, TIME_TO);
		
		return builder;
	}

	public String getPage()
	{
		return page;
	}

	public String getSessionAlias()
	{
		return sessionAlias;
	}

	public String getDirection()
	{
		return direction;
	}

	public FilterForAny<StoredMessageId> getMessageId()
	{
		return messageId;
	}
	
	
	@Override
	public String toString()
	{
		List<String> result = new ArrayList<>(10);
		if (page != null)
			result.add("page=" + page);
		if (sessionAlias != null)
			result.add("sessionAlias=" + sessionAlias);
		if (direction != null)
			result.add("direction=" + direction);
		if (messageTimeFrom != null)
			result.add("timestamp" + messageTimeFrom);
		if (messageTimeTo != null)
			result.add("timestamp" + messageTimeTo);
		if (messageId != null)
			result.add("messageId" + messageId);
		return String.join(", ", result);
	}
}
