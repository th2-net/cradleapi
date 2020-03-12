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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.REPORT_MSGS_LINK_MAX_MSGS;
import static com.exactpro.cradle.cassandra.StorageConstants.ID;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;
import static com.exactpro.cradle.cassandra.StorageConstants.MESSAGES_IDS;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;

public class MessagesLinker
{
	private final QueryExecutor exec;
	private final String keyspace,
			linksTable,
			linkColumn;
	private final UUID instanceId;
	
	public MessagesLinker(QueryExecutor exec, String keyspace, String linksTable, String linkColumn, UUID instanceId)
	{
		this.exec = exec;
		this.keyspace = keyspace;
		this.linksTable = linksTable;
		this.linkColumn = linkColumn;
		this.instanceId = instanceId;
	}
	
	public List<String> linkIdToMessages(String linkedId, Set<StoredMessageId> messagesIds) throws IOException
	{
		List<String> ids = new ArrayList<>();
		List<String> messagesIdsAsStrings = messagesIds.stream().map(StoredMessageId::toString).collect(toList());
		int msgsSize = messagesIdsAsStrings.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + REPORT_MSGS_LINK_MAX_MSGS, msgsSize);
			List<String> curMsgsIds = messagesIdsAsStrings.subList(left, right);
			UUID id = UUID.randomUUID();
			Insert insert = insertInto(keyspace, linksTable)
					.value(ID, literal(id))
					.value(INSTANCE_ID, literal(instanceId))
					.value(linkColumn, literal(UUID.fromString(linkedId)))
					.value(MESSAGES_IDS, literal(curMsgsIds))
					.ifNotExists();
			exec.executeQuery(insert.asCql());
			ids.add(id.toString());
			left = right - 1;
		}
		return ids;
	}

	public String getLinkedIdByMessageId(String messageId) throws IOException
	{
		Select selectFrom = selectFrom(keyspace, linksTable)
				.column(linkColumn)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(MESSAGES_IDS).contains(literal(messageId))
				.allowFiltering();
		
		Row resultRow = exec.executeQuery(selectFrom.asCql()).one();
		if (resultRow == null)
			return null;
		
		UUID id = resultRow.get(linkColumn, GenericType.UUID);
		if (id == null)
			return null;
		
		return id.toString();
	}

	public List<String> getMessageIdsByLinkedId(String linkedId) throws IOException
	{
		Select selectFrom = selectFrom(keyspace, linksTable)
				.column(MESSAGES_IDS)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(linkColumn).isEqualTo(literal(UUID.fromString(linkedId)))
				.allowFiltering();
		
		Iterator<Row> resultIterator = exec.executeQuery(selectFrom.asCql()).iterator();
		List<String> ids = new ArrayList<>();
		while (resultIterator.hasNext())
		{
			List<String> currentMessageIds = resultIterator.next().get(MESSAGES_IDS,
					GenericType.listOf(GenericType.STRING));
			if (currentMessageIds != null)
				ids.addAll(currentMessageIds);
		}
		if (ids.isEmpty())
			return null;
		
		return ids;
	}

	public boolean doMessagesLinkedToIdExist(String linkedId) throws IOException
	{
		Select selectFrom = selectFrom(keyspace, linksTable)
				.column(MESSAGES_IDS)
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId))
				.whereColumn(linksTable).isEqualTo(literal(UUID.fromString(linkedId)))
				.limit(1)
				.allowFiltering();
		
		return exec.executeQuery(selectFrom.asCql()).one() != null;
	}
}
