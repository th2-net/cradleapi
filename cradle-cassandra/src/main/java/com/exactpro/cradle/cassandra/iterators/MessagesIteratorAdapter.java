/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.iterators;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.FilterUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.filters.FilterByField;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;

public class MessagesIteratorAdapter implements Iterable<StoredMessage>
{
	private final ResultSet rs;
	
	public MessagesIteratorAdapter(StoredMessageFilter filter, 
			QueryExecutor exec, String keyspace, String messagesTable, String streamsTable, UUID instanceId) throws IOException
	{
		Select selectFrom = CassandraMessageUtils.prepareSelect(keyspace, messagesTable, instanceId, filter);
		if (filter != null && filter.getStreamName() != null)
		{
			Set<Term> ids = filterByStreamName(filter.getStreamName(), exec, keyspace, streamsTable, instanceId);
			if (ids != null && !ids.isEmpty())
				selectFrom = selectFrom.whereColumn(ID).in(ids);
			else
				selectFrom = selectFrom.whereColumn(ID).isEqualTo(literal(""));
		}
		this.rs = exec.executeQuery(selectFrom.asCql(), false);
	}
	
	@Override
	public Iterator<StoredMessage> iterator()
	{
		return new MessagesIterator(rs.iterator());
	}
	
	
	//FIXME: this is a dirty solution due to current schema that doesn't fit this filter
	private Set<Term> filterByStreamName(FilterByField<String> filter, QueryExecutor exec, String keyspace, String streamsTable, UUID instanceId) throws IOException
	{
		Select selectMessageIds = selectFrom(keyspace, streamsTable)
				.column(MESSAGES_IDS)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
		selectMessageIds = FilterUtils.filterToWhere(filter, selectMessageIds.whereColumn(STREAM_NAME));
		selectMessageIds = selectMessageIds.allowFiltering();
		
		Set<Term> ids = new HashSet<>();
		Iterator<Row> it = exec.executeQuery(selectMessageIds.asCql(), false).iterator();
		while (it.hasNext())
		{
			Row r = it.next();
			List<String> msgIds = r.getList(MESSAGES_IDS, String.class);
			if (msgIds == null)
				continue;
			
			for (String msgId : msgIds)
				ids.add(literal(msgId.split(StoredMessageId.IDS_DELIMITER)[0]));
		}
		return ids;
	}
}