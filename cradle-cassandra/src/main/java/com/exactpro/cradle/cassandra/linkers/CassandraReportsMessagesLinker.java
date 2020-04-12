/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.linkers;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.reports.ReportsMessagesLinker;
import com.exactpro.cradle.reports.StoredReportId;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class CassandraReportsMessagesLinker extends MessagesLinker implements ReportsMessagesLinker
{
	public CassandraReportsMessagesLinker(QueryExecutor exec, String keyspace, String linksTable, String linkColumn,
			UUID instanceId)
	{
		super(exec, keyspace, linksTable, linkColumn, instanceId);
	}

	@Override
	public StoredReportId getReportIdByMessageId(StoredMessageId messageId) throws IOException
	{
		String result = getLinkedByMessageId(messageId);
		return result != null ? new StoredReportId(result) : null;
	}

	@Override
	public List<StoredMessageId> getMessageIdsByReportId(StoredReportId reportId) throws IOException
	{
		return getLinkedMessageIds(reportId.toString());
	}

	@Override
	public boolean isReportLinkedToMessages(StoredReportId reportId) throws IOException
	{
		return isLinkedToMessages(reportId.toString());
	}
}
