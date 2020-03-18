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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.zip.DataFormatException;

import org.apache.commons.lang3.SerializationUtils;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.utils.CradleUtils;

public class MessageUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId)
	{
		return selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
	}
	
	public static byte[] serializeMessages(StoredMessage[] messages, CradleStorage storage) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(out))
		{
			for (StoredMessage msg : messages)
			{
				if (msg == null)  //For case of not full batch
					break;
				StoredMessage copy = new StoredMessage(msg);
				//Replacing stream name with stream ID to be aware of possible stream rename in future
				copy.setStreamName(storage.getStreamId(copy.getStreamName()));
				
				byte[] serializedMsg = SerializationUtils.serialize(copy);
				dos.writeInt(serializedMsg.length);
				dos.write(serializedMsg);
			}
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}
	
	public static StoredMessage deserializeMessage(byte[] contentBytes, int messageIndex, CradleStorage storage) throws IOException
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			int index = -1;
			while (dis.available() != 0)
			{
				index++;
				byte[] messageBytes = readNextMessageBytes(dis);
				if (messageIndex != index)
					continue;
				
				return deserializeMessage(messageBytes, storage);
			}
		}
		
		return null;
	}

	public static Collection<StoredMessage> deserializeMessages(byte[] contentBytes, CradleStorage storage) throws IOException
	{
		List<StoredMessage> storedMessages = new ArrayList<>();
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] messageBytes = readNextMessageBytes(dis);
				StoredMessage tempMessage = deserializeMessage(messageBytes, storage);
				storedMessages.add(tempMessage);
			}
		}
		return storedMessages;
	}
	
	public static StoredMessage toMessage(Row row, int index, CradleStorage storage) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(row);
		return deserializeMessage(contentBytes, index, storage);
	}
	
	public static Collection<StoredMessage> toMessages(Row row, CradleStorage storage) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(row);
		return deserializeMessages(contentBytes, storage);
	}
	
	
	private static byte[] readNextMessageBytes(DataInputStream dis) throws IOException
	{
		int messageSize = dis.readInt();
		byte[] result = new byte[messageSize];
		dis.read(result);
		return result;
	}
	
	private static StoredMessage deserializeMessage(byte[] bytes, CradleStorage storage)
	{
		StoredMessage result = (StoredMessage) SerializationUtils.deserialize(bytes);
		
		//result.streamName contains stream ID. Replacing it with corresponding stream name
		result.setStreamName(storage.getStreamName(result.getStreamName()));
		return result;
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
			return CradleUtils.decompressData(contentBytes);
		}
		catch (IOException | DataFormatException e)
		{
			UUID id = row.get(ID, GenericType.UUID);
			throw new IOException(String.format("Could not decompress batch contents (ID: '%s') from global " +
					"storage",	id), e);
		}
	}
}
