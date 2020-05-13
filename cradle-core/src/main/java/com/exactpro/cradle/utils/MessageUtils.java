package com.exactpro.cradle.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

import org.apache.commons.lang3.SerializationUtils;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;

public class MessageUtils
{
	/**
	 * Serializes messages, skipping non-meaningful or calculatable fields
	 * @param messages to serialize
	 * @return array of bytes, containing serialized messages
	 * @throws IOException if serialization failed
	 */
	public static byte[] serializeMessages(Collection<StoredMessage> messages) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(out))
		{
			for (StoredMessage msg : messages)
			{
				if (msg == null)  //For case of not full batch
					break;
				
				byte[] serializedMsg = SerializationUtils.serialize(msg);
				dos.writeInt(serializedMsg.length);
				dos.write(serializedMsg);
			}
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}
	
	/**
	 * Deserializes messages from given array of bytes till message with needed ID is found
	 * @param contentBytes to deserialize needed message from
	 * @param id of message to find 
	 * @return deserialized message, if found, null otherwise
	 * @throws IOException if deserialization failed
	 */
	public static StoredMessage deserializeOneMessage(byte[] contentBytes, StoredMessageId id) throws IOException
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] messageBytes = readNextMessageBytes(dis);
				StoredMessage msg = deserializeMessage(messageBytes);
				if (id.equals(msg.getId()))
					return msg;
			}
		}
		return null;
	}
	
	/**
	 * Deserializes all messages
	 * @param contentBytes to deserialize messages from
	 * @return collection of deserialized messages
	 * @throws IOException if deserialization failed
	 */
	public static Collection<StoredMessage> deserializeMessages(byte[] contentBytes) throws IOException
	{
		List<StoredMessage> storedMessages = new ArrayList<>();
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] messageBytes = readNextMessageBytes(dis);
				StoredMessage tempMessage = deserializeMessage(messageBytes);
				storedMessages.add(tempMessage);
			}
		}
		return storedMessages;
	}
	
	/**
	 * Decompresses given ByteBuffer and deserializes messages till message with needed ID is found
	 * @param content to deserialize needed message from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @param id of message to find
	 * @return deserialized message, if found, null otherwise
	 * @throws IOException if deserialization failed
	 */
	public static StoredMessage bytesToOneMessage(ByteBuffer content, boolean compressed, StoredMessageId id) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(content, compressed, id.getBatchId());
		return deserializeOneMessage(contentBytes, id);
	}
	
	/**
	 * Decompresses given ByteBuffer and deserializes all messages
	 * @param content to deserialize messages from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @return collection of deserialized messages
	 * @throws IOException if deserialization failed
	 */
	public static Collection<StoredMessage> bytesToMessages(ByteBuffer content, boolean compressed) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(content, compressed, null);
		return deserializeMessages(contentBytes);
	}
	
	
	private static byte[] readNextMessageBytes(DataInputStream dis) throws IOException
	{
		int messageSize = dis.readInt();
		byte[] result = new byte[messageSize];
		dis.read(result);
		return result;
	}
	
	private static StoredMessage deserializeMessage(byte[] bytes)
	{
		return (StoredMessage)SerializationUtils.deserialize(bytes);
	}
	
	private static byte[] getMessageContentBytes(ByteBuffer content, boolean compressed, StoredMessageBatchId batchId) throws IOException
	{
		byte[] contentBytes = content.array();
		if (!compressed)
			return contentBytes;
		
		try
		{
			return CompressionUtils.decompressData(contentBytes);
		}
		catch (IOException | DataFormatException e)
		{
			throw new IOException(String.format("Could not decompress contents of batch (ID: '%s') from Cradle", batchId), e);
		}
	}
}
