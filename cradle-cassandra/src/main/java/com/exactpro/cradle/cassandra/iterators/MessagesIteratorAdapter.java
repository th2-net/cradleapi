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
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.messages.StoredMessage;

public class MessagesIteratorAdapter implements Iterable<StoredMessage>
{
	private final ResultSet rs;
	
	public MessagesIteratorAdapter(QueryExecutor exec, String keyspace, String messagesTable, UUID instanceId) throws IOException
	{
		Select selectFrom = CassandraMessageUtils.prepareSelect(keyspace, messagesTable, instanceId);
		
		this.rs = exec.executeQuery(selectFrom.asCql());
	}
	
	@Override
	public Iterator<StoredMessage> iterator()
	{
		return new MessagesIterator(rs.iterator());
	}
}