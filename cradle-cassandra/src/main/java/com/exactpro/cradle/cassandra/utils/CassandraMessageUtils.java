/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.utils;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.StorageConstants.COMPRESSED;
import static com.exactpro.cradle.cassandra.StorageConstants.CONTENT;
import static com.exactpro.cradle.cassandra.StorageConstants.ID;
import static com.exactpro.cradle.cassandra.StorageConstants.INSTANCE_ID;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.zip.DataFormatException;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.MessageUtils;

public class CassandraMessageUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId)
	{
		return selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
	}
	
	public static StoredMessage toMessage(Row row, StoredMessageId id) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(row);
		return MessageUtils.deserializeOneMessage(contentBytes, id);
	}
	
	public static Collection<StoredMessage> toMessages(Row row) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(row);
		return MessageUtils.deserializeMessages(contentBytes, new StoredMessageBatchId(row.getString(ID)));
	}
	
	
	private static byte[] getMessageContentBytes(Row row) throws IOException
	{
		Boolean compressed = row.get(COMPRESSED, GenericType.BOOLEAN);
		
		ByteBuffer contentByteBuffer = row.get(CONTENT, GenericType.BYTE_BUFFER);
		byte[] contentBytes = contentByteBuffer.array();
		if (!Boolean.TRUE.equals(compressed))
			return contentBytes;
		
		try
		{
			return CompressionUtils.decompressData(contentBytes);
		}
		catch (IOException | DataFormatException e)
		{
			String id = row.getString(ID);
			throw new IOException(String.format("Could not decompress batch contents (ID: '%s') from global " +
					"storage",	id), e);
		}
	}
}
