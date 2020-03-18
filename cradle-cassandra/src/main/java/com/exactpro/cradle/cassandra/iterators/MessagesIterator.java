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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.cassandra.utils.MessageUtils;

public class MessagesIterator implements Iterator<StoredMessage>
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesIterator.class);
	
	private final Iterator<Row> rows;
	private final CradleStorage storage;
	private Iterator<StoredMessage> batchIterator;
	
	public MessagesIterator(Iterator<Row> rows, CradleStorage storage)
	{
		this.rows = rows;
		this.storage = storage;
	}
	
	@Override
	public boolean hasNext()
	{
		if (batchIterator != null)
		{
			if (batchIterator.hasNext())
				return true;
			batchIterator = null;
		}
		return rows.hasNext();
	}
	
	@Override
	public StoredMessage next()
	{
		if (batchIterator != null)
			return batchIterator.next();
		
		Row r = rows.next();
		try
		{
			batchIterator = MessageUtils.toMessages(r, storage).iterator();
			if (batchIterator.hasNext())
				return batchIterator.next();
			batchIterator = null;
			return null;
		}
		catch (IOException e)
		{
			logger.warn("Error while getting message", e);
			return null;
		}
	}
}