package com.exactpro.cradle.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;

public class MessageUtils
{
	public static byte[] serializeMessages(StoredMessage[] messages) throws IOException
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
	
	public static StoredMessage deserializeOneMessage(byte[] contentBytes, StoredMessageId id) throws IOException
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			int index = -1;
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

	public static Collection<StoredMessage> deserializeMessages(byte[] contentBytes, StoredMessageBatchId batchId) throws IOException
	{
		List<StoredMessage> storedMessages = new ArrayList<>();
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			int index = -1;
			while (dis.available() != 0)
			{
				index++;
				byte[] messageBytes = readNextMessageBytes(dis);
				StoredMessage tempMessage = deserializeMessage(messageBytes, new StoredMessageId(batchId, index));
				storedMessages.add(tempMessage);
			}
		}
		return storedMessages;
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
		result.setId(id);
		return result;
	}
}
