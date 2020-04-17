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
import java.util.Collection;
import java.util.Iterator;

import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.messages.StoredMessage;

public class MessagesIterator extends EntityIterator<StoredMessage>
{
	public MessagesIterator(Iterator<Row> rows)
	{
		super(rows, "message");
	}
	
	
	@Override
	protected Collection<StoredMessage> rowToCollection(Row row) throws IOException
	{
		return CassandraMessageUtils.toMessages(row);
	}
}