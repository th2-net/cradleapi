/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.utils;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.CradleMessage;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.MessageCommonParams;
import com.exactpro.cradle.serialization.MessageDeserializer;
import com.exactpro.cradle.serialization.MessageSerializer;
import com.exactpro.cradle.serialization.SerializedEntityData;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

public class MessageUtils
{

	private static final MessageSerializer serializer = new MessageSerializer();
	private static final MessageDeserializer deserializer = new MessageDeserializer();

	/**
	 * Checks that message has all necessary fields set
	 * @param message to validate
	 * @throws CradleStorageException if validation failed
	 */
	public static void validateMessage(CradleMessage message) throws CradleStorageException
	{
		if (message.getId() == null)
			throw new CradleStorageException("Messages ID cannot be null");
		if (message.getBookId() == null || StringUtils.isEmpty(message.getBookId().toString()))
			throw new CradleStorageException("Message must have a book");
		if (StringUtils.isEmpty(message.getSessionAlias()))
			throw new CradleStorageException("Message must have a session alias");
		if (message.getDirection() == null)
			throw new CradleStorageException("Message must have a direction");
		if (message.getTimestamp() == null)
			throw new CradleStorageException("Message must have a timestamp");
		if (ArrayUtils.isEmpty(message.getContent()))
			throw new CradleStorageException("Message must have content");
	}

	/**
	 * Serializes batch messages, skipping non-meaningful or calculable fields
	 *
	 * @param batch to serialize
	 * @return {@link SerializedEntityData} containing serialized messages.
	 * @throws IOException if serialization failed
	 */
	public static SerializedEntityData serializeMessages(MessageBatchToStore batch) throws IOException {
		return serializer.serializeBatch(batch);
	}

	/**
	 * Serializes batch messages, skipping non-meaningful or calculable fields
	 *
	 * @param batch to serialize
	 * @return {@link SerializedEntityData} containing serialized messages.
	 * @throws IOException if serialization failed
	 */
	public static SerializedEntityData serializeMessages(GroupedMessageBatchToStore batch) throws IOException {
		return serializer.serializeBatch(batch);
	}

	/**
	 * Serializes messages, skipping non-meaningful or calculable fields
	 *
	 * @param messages to serialize
	 * @return {@link SerializedEntityData} containing serialized messages.
	 * @throws IOException if serialization failed
	 */
	public static SerializedEntityData serializeMessages(Collection<StoredMessage> messages) throws IOException {
		return serializer.serializeBatch(messages);
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
		return deserializer.deserializeOneMessage(ByteBuffer.wrap(contentBytes), new MessageCommonParams(id), id);
	}
	
	/**
	 * Deserializes all messages
	 * @param contentBytes to deserialize messages from
	 * @param bookId to deserialize messages from
	 * @return collection of deserialized messages
	 * @throws IOException if deserialization failed
	 */
	public static List<StoredMessage> deserializeMessages(byte[] contentBytes, BookId bookId) throws IOException
	{
		return deserializer.deserializeBatch(contentBytes, new MessageCommonParams(bookId));
	}

	/**
	 * Deserializes all messages
	 * @param contentBytes to deserialize messages from
	 * @param batchId to deserialize messages from
	 * @return collection of deserialized messages
	 * @throws IOException if deserialization failed
	 */
	public static List<StoredMessage> deserializeMessages(byte[] contentBytes, StoredMessageId batchId) throws IOException
	{
		return deserializer.deserializeBatch(contentBytes, new MessageCommonParams(batchId));
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
		byte[] contentBytes = getMessageContentBytes(content, compressed, id);
		return deserializeOneMessage(contentBytes, id);
	}
	
	/**
	 * Decompresses given ByteBuffer and deserializes all messages
	 * @param content to deserialize messages from
	 * @param id batch id. Required to specify common message params like bookId, sessionAlias, direction
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @return collection of deserialized messages
	 * @throws IOException if deserialization failed
	 */
	public static List<StoredMessage> bytesToMessages(ByteBuffer content, StoredMessageId id, boolean compressed) throws IOException
	{
		byte[] contentBytes = getMessageContentBytes(content, compressed, id);
		return deserializeMessages(contentBytes, id);
	}
	
	private static byte[] getMessageContentBytes(ByteBuffer content, boolean compressed, StoredMessageId id) throws IOException
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
			throw new IOException(String.format("Could not decompress content of message (ID: '%s') from Cradle", id), e);
		}
	}
}
