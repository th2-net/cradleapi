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

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.cassandra.dao.MessageBatchConverter;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageFilter;

public class MessagesIteratorAdapter implements Iterable<StoredMessage>
{
	private final ResultSet rs;
	private final MessageBatchConverter converter;
	
	public MessagesIteratorAdapter(StoredMessageFilter filter, String keyspace, String messagesTable, 
			QueryExecutor exec, UUID instanceId, MessageBatchConverter converter) throws IOException
	{
		Select select = CassandraMessageUtils.prepareSelect(keyspace, messagesTable, instanceId);
		this.rs = selectByFilter(select, filter, exec);
		this.converter = converter;
	}
	
	@Override
	public Iterator<StoredMessage> iterator()
	{
		return new MessagesIterator(rs.iterator(), converter);
	}
	
	
	private ResultSet selectByFilter(Select select, StoredMessageFilter filter, QueryExecutor exec) throws IOException
	{
		if (filter != null)
			select = CassandraMessageUtils.applyFilter(select, filter);
		select = select.allowFiltering();
		return exec.executeQuery(select.asCql(), false);
	}
}