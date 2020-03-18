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
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.cassandra.utils.MessageUtils;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class MessagesIteratorAdapter implements Iterable<StoredMessage>
{
	private final ResultSet rs;
	private final CradleStorage storage;
	
	public MessagesIteratorAdapter(QueryExecutor exec, String keyspace, String messagesTable, UUID instanceId,
			CradleStorage storage) throws IOException
	{
		Select selectFrom = MessageUtils.prepareSelect(keyspace, messagesTable, instanceId);
		
		this.rs = exec.executeQuery(selectFrom.asCql());
		this.storage = storage;
	}
	
	@Override
	public Iterator<StoredMessage> iterator()
	{
		return new MessagesIterator(rs.iterator(), storage);
	}
}