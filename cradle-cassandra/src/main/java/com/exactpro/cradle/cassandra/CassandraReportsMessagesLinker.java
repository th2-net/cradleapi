/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.exactpro.cradle.ReportsMessagesLinker;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CassandraReportsMessagesLinker extends MessagesLinker implements ReportsMessagesLinker
{
	public CassandraReportsMessagesLinker(QueryExecutor exec, String keyspace, String linksTable, String linkColumn,
			UUID instanceId)
	{
		super(exec, keyspace, linksTable, linkColumn, instanceId);
	}

	@Override
	public String getReportIdByMessageId(StoredMessageId messageId) throws IOException
	{
		return getLinkedIdByMessageId(messageId);
	}

	@Override
	public List<StoredMessageId> getMessageIdsByReportId(String reportId) throws IOException
	{
		return getMessageIdsByLinkedId(reportId);
	}

	@Override
	public boolean isReportLinkedToMessages(String reportId) throws IOException
	{
		return isLinkedToMessages(reportId);
	}
}
