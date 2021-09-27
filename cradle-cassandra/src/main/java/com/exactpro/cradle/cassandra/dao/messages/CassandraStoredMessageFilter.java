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
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.TimeUtils;

import java.time.Instant;
import java.time.LocalDateTime;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.exactpro.cradle.cassandra.StorageConstants.*;
import static com.exactpro.cradle.cassandra.StorageConstants.PART;
import static com.exactpro.cradle.filters.ComparisonOperation.*;

public class CassandraStoredMessageFilter implements CassandraFilter<MessageBatchEntity>
{
	private static final String DATE_FROM = "dateFrom", DATE_TO = "dateTo",
			TIME_FROM = "timeFrom", TIME_TO = "timeTo";

	private final String page, sessionAlias, direction, part;

	private final FilterForGreater<Instant> messageTimeFrom;
	private final FilterForLess<Instant> messageTimeTo;
	private final FilterForAny<StoredMessageId> messageId;
	private boolean isTimeFromBounded = false, isTimeToBounded = false;

	public CassandraStoredMessageFilter(String page, String sessionAlias, String direction, String part,
			FilterForGreater<Instant> messageTimeFrom, FilterForLess<Instant> messageTimeTo,
			FilterForAny<StoredMessageId> messageId)
	{
		this.page = page;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		this.part = part;
		this.messageTimeFrom = messageTimeFrom;
		this.messageTimeTo = messageTimeTo;
		this.messageId = messageId;
	}

	@Override
	public Select addConditions(Select select)
	{
		select = select.whereColumn(PAGE).isEqualTo(bindMarker())
			.whereColumn(SESSION_ALIAS).isEqualTo(bindMarker())
			.whereColumn(DIRECTION).isEqualTo(bindMarker())
			.whereColumn(PART).isEqualTo(bindMarker());

		if (messageId != null)
			select = addMessageIdCondition(select);

		if (!isTimeFromBounded && messageTimeFrom != null)
			select = addDateTimeCondition(select, messageTimeFrom.getOperation(), DATE_FROM, TIME_FROM);

		if (!isTimeToBounded && messageTimeTo != null)
			select = addDateTimeCondition(select, messageTimeTo.getOperation(), DATE_TO, TIME_TO);

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
				select = mcrBuilder.isGreaterThanOrEqualTo(tuple(bindMarker(DATE_TO), bindMarker(TIME_TO), bindMarker(SEQUENCE)));
				isTimeFromBounded = true;
		}
		return select;
	}

	private Select addDateTimeCondition(Select select, ComparisonOperation op, String dateMarker, String timeMarker)
	{
		MultiColumnRelationBuilder<Select> mcrBuilder = select.whereColumns(MESSAGE_DATE, MESSAGE_TIME);
		switch (op)
		{
			case LESS:
				select = mcrBuilder.isLessThan(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
				break;
			case GREATER:
				select = mcrBuilder.isGreaterThan(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
				break;
			case LESS_OR_EQUALS:
				select = mcrBuilder.isLessThanOrEqualTo(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
				break;
			case GREATER_OR_EQUALS:
				select = mcrBuilder.isGreaterThanOrEqualTo(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
				break;
			case EQUALS:
				select = mcrBuilder.isEqualTo(tuple(bindMarker(dateMarker), bindMarker(timeMarker)));
				break;
		}
		return select;
	}

	@Override
	public BoundStatementBuilder bindParameters(BoundStatementBuilder builder)
	{
		builder = builder.setString(PAGE, page)
				.setString(SESSION_ALIAS, sessionAlias)
				.setString(DIRECTION, direction)
				.setString(PART, part);

		Instant timestampFromId = null;
		if (messageId != null)
		{
			timestampFromId = messageId.getValue().getTimestamp();
			LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestampFromId);
			if (messageId.getOperation() == LESS || messageId.getOperation() == LESS_OR_EQUALS)
			{

				builder = builder.setLocalDate(DATE_TO, ldt.toLocalDate());
				builder = builder.setLocalTime(TIME_TO, ldt.toLocalTime());
				builder = builder.setLong(SEQUENCE, messageId.getValue().getSequence());
			}
			else
			{
				builder = builder.setLocalDate(DATE_FROM, ldt.toLocalDate());
				builder = builder.setLocalTime(TIME_FROM, ldt.toLocalTime());
				builder = builder.setLong(SEQUENCE, messageId.getValue().getSequence());
			}
		}

		if (messageTimeFrom != null && (timestampFromId == null || messageTimeFrom.getValue().isAfter(timestampFromId)))
		{
			LocalDateTime ldt = TimeUtils.toLocalTimestamp(messageTimeFrom.getValue());
			builder = builder.setLocalDate(DATE_FROM, ldt.toLocalDate());
			builder = builder.setLocalTime(TIME_FROM, ldt.toLocalTime());
		}

		if (messageTimeTo != null && (timestampFromId == null || messageTimeTo.getValue().isBefore(timestampFromId)))
		{
			LocalDateTime ldt = TimeUtils.toLocalTimestamp(messageTimeTo.getValue());
			builder = builder.setLocalDate(DATE_TO, ldt.toLocalDate());
			builder = builder.setLocalTime(TIME_TO, ldt.toLocalTime());
		}

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

	public String getPart()
	{
		return part;
	}

	public FilterForAny<StoredMessageId> getMessageId()
	{
		return messageId;
	}
}
