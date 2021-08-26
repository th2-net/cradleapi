/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.zip.DataFormatException;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.TestEvent;
import com.exactpro.cradle.testevents.TestEventBatch;
import com.exactpro.cradle.testevents.TestEventSingle;

public class TestEventUtils
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventUtils.class);
	
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
		
		if (event.getBookId() == null)
			throw new CradleStorageException("Test event must have a book");
		if (event.getScope() == null)
			throw new CradleStorageException("Test event must have a scope");
		if (event.getStartTimestamp() == null)
			throw new CradleStorageException("Test event must have a start timestamp");
		
		if (event.getEndTimestamp() != null && event.getEndTimestamp().isBefore(event.getStartTimestamp()))
			throw new CradleStorageException("Test event cannot end sooner than it started");
		
		if (event.getParentId() != null && !event.getBookId().equals(event.getParentId().getBookId()))
			throw new CradleStorageException("Test event and its parent must be from the same book");
		
		Set<StoredMessageId> messages = event.getMessages();
		if (messages != null)
			validateMessages(messages, event.getBookId());
	}
	
	/**
	 * Serializes test events, skipping non-meaningful or calculatable fields
	 * @param testEvents to serialize
	 * @return array of bytes, containing serialized events
	 * @throws IOException if serialization failed
	 */
	public static byte[] serializeTestEvents(Collection<BatchedStoredTestEvent> testEvents) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(out))
		{
			for (BatchedStoredTestEvent te : testEvents)
				serialize(te, dos);
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}
	
	/**
	 * Deserializes test events from given bytes
	 * @param contentBytes to deserialize events from
	 * @return collection of deserialized test events
	 * @throws IOException if deserialization failed
	 */
	public static Collection<BatchedStoredTestEvent> deserializeTestEvents(byte[] contentBytes)
			throws IOException, CradleStorageException
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			Collection<BatchedStoredTestEvent> result = new ArrayList<>();
			while (dis.available() != 0)
			{
				byte[] teBytes = readNextData(dis);
				BatchedStoredTestEvent tempTe = deserializeTestEvent(teBytes);
				result.add(tempTe);
			}
			return result;
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
	public static byte[] getTestEventContent(StoredTestEvent event) throws IOException
	{
		if (event.isBatch())
		{
			logger.trace("Serializing children of test event batch '{}'", event.getId());
			return serializeTestEvents(event.asBatch().getTestEvents());
		}
		return event.asSingle().getContent();
	}
	
	
	private static void serialize(Serializable data, DataOutputStream target) throws IOException
	{
		byte[] serializedData = SerializationUtils.serialize(data);
		target.writeInt(serializedData.length);
		target.write(serializedData);
	}
	
	private static byte[] readNextData(DataInputStream source) throws IOException
	{
		int size = source.readInt();
		byte[] result = new byte[size];
		source.read(result);
		return result;
	}
	
	private static BatchedStoredTestEvent deserializeTestEvent(byte[] bytes)
	{
		return (BatchedStoredTestEvent)SerializationUtils.deserialize(bytes);
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
