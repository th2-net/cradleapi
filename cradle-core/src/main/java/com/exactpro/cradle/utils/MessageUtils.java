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
	 * Deserializes given array of bytes till message with needed index is found
	 * @param contentBytes to deserialize needed message from
	 * @param id of message to find. Message is found if index from ID matches index of read message 
	 * @return deserialized message with ID assigned, if found, null otherwise
	 * @throws IOException if deserialization failed
	 */
	public static StoredMessage deserializeOneMessage(byte[] contentBytes, StoredMessageId id) throws IOException
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			long index = id.getBatchId().getIndex()-1;
			while (dis.available() != 0)
			{
				index++;
				byte[] messageBytes = readNextMessageBytes(dis);
				if (id.getIndex() != index)
					continue;
				
				return deserializeMessage(messageBytes, id);
			}
		}
		
		return null;
	}
	
	/**
	 * Deserializes all messages, assigning them proper IDs based on given batchId
	 * @param contentBytes to deserialize messages from
	 * @param batchId to use to build message IDs
	 * @return collection of deserialized messages
	 * @throws IOException if deserialization failed
	 */
	public static Collection<StoredMessage> deserializeMessages(byte[] contentBytes, StoredMessageBatchId batchId) throws IOException
	{
		List<StoredMessage> storedMessages = new ArrayList<>();
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			long index = batchId.getIndex()-1;
			while (dis.available() != 0)
			{
				index++;
				byte[] messageBytes = readNextMessageBytes(dis);
				StoredMessage tempMessage = deserializeMessage(messageBytes, 
						new StoredMessageId(batchId, index));
				storedMessages.add(tempMessage);
			}
		}
		return storedMessages;
	}
	
	/**
	 * Decompresses given ByteBuffer and deserializes messages till message with needed index is found
	 * @param content to deserialize needed message from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @param id of message to find. Message is found if index from ID matches index of read message
	 * @return deserialized message with ID assigned, if found, null otherwise
	 * @throws IOException if deserialization failed
	 */
	public static StoredMessage bytesToOneMessage(ByteBuffer content, boolean compressed, StoredMessageId id) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(content, compressed, id.getBatchId());
		return deserializeOneMessage(contentBytes, id);
	}
	
	/**
	 * Decompresses given ByteBuffer and deserializes all messages, assigning them proper IDs based on given batchId
	 * @param content to deserialize messages from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @param batchId to use to build message IDs
	 * @return collection of deserialized messages
	 * @throws IOException if deserialization failed
	 */
	public static Collection<StoredMessage> bytesToMessages(ByteBuffer content, boolean compressed, StoredMessageBatchId batchId) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(content, compressed, batchId);
		return deserializeMessages(contentBytes, batchId);
	}
	
	
	private static byte[] readNextMessageBytes(DataInputStream dis) throws IOException
	{
		int messageSize = dis.readInt();
		byte[] result = new byte[messageSize];
		dis.read(result);
		return result;
	}
	
	private static StoredMessage deserializeMessage(byte[] bytes, StoredMessageId id)
	{
		StoredMessage result = (StoredMessage)SerializationUtils.deserialize(bytes);
		return new StoredMessage(result, id);
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
