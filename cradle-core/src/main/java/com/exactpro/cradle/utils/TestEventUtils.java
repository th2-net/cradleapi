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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;

import com.exactpro.cradle.serialization.*;
import com.exactpro.cradle.testevents.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;

public class TestEventUtils
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventUtils.class);

	private static final EventBatchDeserializer deserializer = new EventBatchDeserializer();
	private static final EventBatchSerializer serializer = new EventBatchSerializer();
	
	/**
	 * Checks that test event has all necessary fields set
	 * @param event to validate
	 * @throws CradleStorageException if validation failed
	 */
	public static void validateTestEvent(TestEvent event) throws CradleStorageException
	{
		if (event.getId() == null)
			throw new CradleStorageException("Test event ID cannot be null");
		
		if (event.getId().equals(event.getParentId()))
			throw new CradleStorageException("Test event cannot reference itself");
		
		if (event instanceof TestEventSingle && StringUtils.isEmpty(event.getName()))
			throw new CradleStorageException("Single test event must have a name");
		if (event instanceof TestEventBatch && event.getParentId() == null)
			throw new CradleStorageException("Batch must have a parent");
		
		if (event.getBookId() == null || StringUtils.isEmpty(event.getBookId().toString()))
			throw new CradleStorageException("Test event must have a book");
		if (StringUtils.isEmpty(event.getScope()))
			throw new CradleStorageException("Test event must have a scope");
		if (event.getStartTimestamp() == null)
			throw new CradleStorageException("Test event must have a start timestamp");
		Instant now = Instant.now();
		if (event.getStartTimestamp().isAfter(now))
			throw new CradleStorageException(
					"Event start timestamp (" + TimeUtils.toLocalTimestamp(event.getStartTimestamp()) +
							") is greater than current timestamp (" + TimeUtils.toLocalTimestamp(now) + ")");
		validateTestEventEndDate(event);
		if (event.getParentId() != null && !event.getBookId().equals(event.getParentId().getBookId()))
			throw new CradleStorageException("Test event and its parent must be from the same book");
		
		Set<StoredMessageId> messages = event.getMessages();
		if (messages != null)
			validateMessages(messages, event.getBookId());
	}

	/**
	 * Validate that end timestamp of test event is greater than start timestamp
	 * @param event to validate
	 * @throws CradleStorageException if validation failed
	 */
	public static void validateTestEventEndDate(TestEvent event) throws CradleStorageException
	{
		if (event.getEndTimestamp() != null && event.getEndTimestamp().isBefore(event.getStartTimestamp()))
			throw new CradleStorageException("Test event cannot end sooner than it started");
	}
	
	/**
	 * Serializes test events, skipping non-meaningful or calculatable fields
	 * @param testEvents to serialize
	 * @return array of bytes, containing serialized events
	 * @throws IOException if serialization failed
	 */
	public static byte[] serializeTestEvents(Collection<BatchedStoredTestEvent> testEvents) throws IOException
	{
		return serializer.serializeEventBatch(testEvents);
	}
	
	/**
	 * Deserializes test events from given bytes
	 * @param contentBytes to deserialize events from
	 * @return collection of deserialized test events
	 * @throws IOException if deserialization failed
	 */
	public static Collection<BatchedStoredTestEvent> deserializeTestEvents(byte[] contentBytes)
			throws IOException
	{
		if (deserializer.checkEventBatchHeader(contentBytes)) {
			return deserializer.deserializeBatchEntries(contentBytes);
		} else {
			//This code is for backward compatibility.
			return LegacyEventDeserializer.deserializeTestEvents(contentBytes);
		}
	}
	
	
	/**
	 * Decompresses given ByteBuffer and deserializes test events
	 * @param content to deserialize events from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @return collection of deserialized test events
	 * @throws IOException if deserialization failed
	 * @throws CradleStorageException if deserialized event doesn't match batch conditions
	 */
	public static Collection<BatchedStoredTestEvent> bytesToTestEvents(ByteBuffer content, boolean compressed)
			throws IOException, CradleStorageException
	{
		byte[] contentBytes = getTestEventContentBytes(content, compressed);
		return deserializeTestEvents(contentBytes);
	}
	
	public static byte[] getTestEventContentBytes(ByteBuffer content, boolean compressed) throws IOException
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
			throw new IOException("Could not decompress content of test event", e);
		}
	}
	
	
	/**
	 * Returns content of given test event as bytes. If the event is a batch, child events are serialized and returned as bytes
	 * @param event whose content to get
	 * @return bytes of test event content
	 * @throws IOException if batch children serialization failed
	 */
	public static byte[] getTestEventContent(TestEventToStore event) throws IOException
	{
		if (event.isBatch())
		{
			logger.trace("Serializing children of test event batch '{}'", event.getId());
			return serializeTestEvents(event.asBatch().getTestEvents());
		}
		return event.asSingle().getContent();
	}
	
	
	public static byte[] serializeLinkedMessageIds(TestEventToStore event) throws IOException
	{
		if (event.isBatch())
			return EventMessageIdSerializer.serializeBatchLinkedMessageIds(event.asBatch().getBatchMessages());
		return EventMessageIdSerializer.serializeLinkedMessageIds(event.asSingle().getMessages());
	}

	public static Set<StoredMessageId> deserializeLinkedMessageIds(byte[] bytes, BookId bookId) throws IOException {
		return EventMessageIdDeserializer.deserializeLinkedMessageIds(bytes, bookId);
	}

	public static byte[] serializeLinkedMessageIds(Set<StoredMessageId> messageIds) throws IOException {
		return EventMessageIdSerializer.serializeLinkedMessageIds(messageIds);
	}

	public static Map<StoredTestEventId, Set<StoredMessageId>> deserializeBatchLinkedMessageIds(byte[] bytes, BookId bookId) throws IOException {
		return EventMessageIdDeserializer.deserializeBatchLinkedMessageIds(bytes, bookId);
	}

	public static byte[] serializeBatchLinkedMessageIds(Map<StoredTestEventId, Set<StoredMessageId>> ids) throws IOException {
		return EventMessageIdSerializer.serializeBatchLinkedMessageIds(ids);
	}
	
	private static void validateMessages(Set<StoredMessageId> messages, BookId book) throws CradleStorageException
	{
		for (StoredMessageId id : messages)
		{
			if (!id.getBookId().equals(book))
				throw new CradleStorageException("Book of message '"+id+"' differs from test event book ("+book+")");
		}
	}
}
