/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
import com.datastax.oss.driver.api.querybuilder.relation.MultiColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Order;
import com.exactpro.cradle.cassandra.dao.CassandraFilter;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.filters.ComparisonOperation;
import com.exactpro.cradle.filters.FilterForAny;
import com.exactpro.cradle.filters.FilterForGreater;
import com.exactpro.cradle.filters.FilterForLess;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;

import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.*;

public class CassandraStoredMessageFilter implements CassandraFilter<MessageBatchEntity>
{
	public static final String DATE_FROM = "dateFrom", DATE_TO = "dateTo",
			TIME_FROM = "timeFrom", TIME_TO = "timeTo",
			SEQ_FROM = "seqFrom", SEQ_TO = "seqTo";

	private final String book, page, sessionAlias, direction;

	private final FilterForGreater<Instant> messageTimeFrom;
	private final FilterForLess<Instant> messageTimeTo;
	private final FilterForAny<Long> sequence;

	/** limit must be strictly positive ( limit greater than 0 ) */
	private final int limit;

	private final Order order;

	public CassandraStoredMessageFilter(String book, String page, String sessionAlias, String direction,
										FilterForGreater<Instant> messageTimeFrom, FilterForLess<Instant> messageTimeTo)
	{
		this.book = book;
		this.page = page;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		this.messageTimeFrom = messageTimeFrom;
		this.messageTimeTo = messageTimeTo;
		this.sequence = null;
		this.limit = 0;
		this.order = Order.DIRECT;
	}

	public CassandraStoredMessageFilter(String book, String page, String sessionAlias, String direction,
										FilterForGreater<Instant> messageTimeFrom, FilterForLess<Instant> messageTimeTo, int limit, Order order)
	{
		this.book = book;
		this.page = page;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		this.messageTimeFrom = messageTimeFrom;
		this.messageTimeTo = messageTimeTo;
		this.sequence = null;
		this.limit = limit;
		this.order = order;
	}

	@Override
	public Select addConditions(Select select)
	{
		select = select
				.whereColumn(FIELD_BOOK).isEqualTo(bindMarker())
				.whereColumn(FIELD_PAGE).isEqualTo(bindMarker())
				.whereColumn(FIELD_SESSION_ALIAS).isEqualTo(bindMarker())
				.whereColumn(FIELD_DIRECTION).isEqualTo(bindMarker());
		
		if (sequence != null)
			select = addMessageIdConditions(select);
		else
		{
			if (messageTimeFrom != null)
				select = FilterUtils.timestampFilterToWhere(messageTimeFrom.getOperation(), select, FIELD_FIRST_MESSAGE_DATE, FIELD_FIRST_MESSAGE_TIME, DATE_FROM, TIME_FROM);
			if (messageTimeTo != null)
				select = FilterUtils.timestampFilterToWhere(messageTimeTo.getOperation(), select, FIELD_FIRST_MESSAGE_DATE, FIELD_FIRST_MESSAGE_TIME, DATE_TO, TIME_TO);
		}

		ClusteringOrder orderBy = (order == Order.REVERSE) ? ClusteringOrder.DESC : ClusteringOrder.ASC;
		select = select.orderBy(FIELD_FIRST_MESSAGE_DATE, orderBy)
				.orderBy(FIELD_FIRST_MESSAGE_TIME, orderBy)
				.orderBy(FIELD_SEQUENCE, orderBy);

		if (limit > 0) {
			select = select.limit(limit);
		}

		return select;
	}

	@Override
	public BoundStatementBuilder bindParameters(BoundStatementBuilder builder)
	{
		builder = builder
				.setString(FIELD_BOOK, book)
				.setString(FIELD_PAGE, page)
				.setString(FIELD_SESSION_ALIAS, sessionAlias)
				.setString(FIELD_DIRECTION, direction);
		
		if (sequence != null)
			builder = bindMessageIdParameters(builder);
		else
		{
			if (messageTimeFrom != null)
				builder = FilterUtils.bindTimestamp(messageTimeFrom.getValue(), builder, DATE_FROM, TIME_FROM);
			if (messageTimeTo != null)
				builder = FilterUtils.bindTimestamp(messageTimeTo.getValue(), builder, DATE_TO, TIME_TO);
		}
		return builder;
	}

	public String getBook()
	{
		return book;
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

	@Deprecated
	public FilterForAny<Long> getSequence()
	{
		return sequence;
	}
	
	
	@Override
	public String toString()
	{
		List<String> result = new ArrayList<>(10);
		if (book != null)
			result.add("book=" + book);
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
		if (sequence != null)
			result.add("sequence" + sequence);
		return String.join(", ", result);
	}
	
	
	private MultiColumnRelationBuilder<Select> selectWithMessageId(Select select)
	{
		return select.whereColumns(FIELD_FIRST_MESSAGE_DATE, FIELD_FIRST_MESSAGE_TIME, FIELD_SEQUENCE);
	}
	
	private Select addMessageIdConditions(Select select)
	{
		ComparisonOperation op = sequence.getOperation();
		switch (op)
		{
			case LESS:
			case LESS_OR_EQUALS:
				select = selectWithMessageId(select).isLessThanOrEqualTo(tuple(bindMarker(DATE_TO), bindMarker(TIME_TO), bindMarker(SEQ_TO)));
				if (messageTimeFrom != null)
					select = FilterUtils.timestampFilterToWhere(ComparisonOperation.GREATER_OR_EQUALS, select, FIELD_FIRST_MESSAGE_DATE, FIELD_FIRST_MESSAGE_TIME, DATE_FROM, TIME_FROM);
				break;
			default:
				select = selectWithMessageId(select).isGreaterThanOrEqualTo(tuple(bindMarker(DATE_FROM), bindMarker(TIME_FROM), bindMarker(SEQ_FROM)));
				if (messageTimeTo != null)
					select = FilterUtils.timestampFilterToWhere(ComparisonOperation.LESS_OR_EQUALS, select, FIELD_FIRST_MESSAGE_DATE, FIELD_FIRST_MESSAGE_TIME, DATE_TO, TIME_TO);
		}
		return select;
	}
	
	private BoundStatementBuilder bindMessageIdParameters(BoundStatementBuilder builder)
	{
		ComparisonOperation op = sequence.getOperation();
		switch (op)
		{
			case LESS:
			case LESS_OR_EQUALS:
				Instant to = messageTimeTo != null ? messageTimeTo.getValue() : Instant.MAX;
				builder = FilterUtils.bindTimestamp(to, builder, DATE_TO, TIME_TO)
						.setLong(SEQ_TO, sequence.getValue());
				if (messageTimeFrom != null)
					builder = FilterUtils.bindTimestamp(messageTimeFrom.getValue(), builder, DATE_FROM, TIME_FROM);
				break;
			default:
				Instant from = messageTimeFrom != null ? messageTimeFrom.getValue() : Instant.MIN;
				builder = FilterUtils.bindTimestamp(from, builder, DATE_FROM, TIME_FROM)
						.setLong(SEQ_FROM, sequence.getValue());
				if (messageTimeTo != null)
					builder = FilterUtils.bindTimestamp(messageTimeTo.getValue(), builder, DATE_TO, TIME_TO);
		}
		return builder;
	}
}
