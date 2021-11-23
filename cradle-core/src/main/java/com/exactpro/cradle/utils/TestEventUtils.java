/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.DataFormatException;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.EventBatchDeserializer;
import com.exactpro.cradle.serialization.EventBatchSerializer;
import com.exactpro.cradle.serialization.EventMessageIdDeserializer;
import com.exactpro.cradle.serialization.EventMessageIdSerializer;
import com.exactpro.cradle.serialization.LegacyEventDeserializer;
import com.exactpro.cradle.serialization.SerializationConsumer;
import com.exactpro.cradle.testevents.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class TestEventUtils
{
	
	public static final EventBatchDeserializer deserializer = new EventBatchDeserializer();
	public static final EventBatchSerializer serializer = new EventBatchSerializer();
	
	/**
	 * Checks that test event has all necessary fields set
	 * @param event to validate
	 * @param checkName indicates whether event name should be validated. For some events name is optional and thus shouldn't be checked
	 * @throws CradleStorageException if validation failed
	 */
	public static void validateTestEvent(StoredTestEvent event, boolean checkName) throws CradleStorageException
	{
		if (event.getId() == null)
			throw new CradleStorageException("Test event ID cannot be null");
		if (event.getId().equals(event.getParentId()))
			throw new CradleStorageException("Test event cannot reference itself");
		if (checkName && StringUtils.isEmpty(event.getName()))
			throw new CradleStorageException("Test event must have a name");
		if (event.getStartTimestamp() == null)
			throw new CradleStorageException("Test event must have a start timestamp");
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
	 * Serializes test events metadata, skipping non-meaningful or calculatable fields
	 * @param testEventsMetadata to serialize
	 * @return array of bytes, containing serialized and compressed metadata of events
	 * @throws IOException if serialization failed
	 */
	public static byte[] serializeTestEventsMetadata(Collection<BatchedStoredTestEventMetadata> testEventsMetadata) throws IOException
	{
		return serializer.serializeEventMetadataBatch(testEventsMetadata);
	}
	
	/**
	 * Deserializes all test events, adding them to given batch
	 * @param contentBytes to deserialize events from
	 * @param batch to add events to
	 * @param ids Map of Collection of messages' id's related with added events
	 * @throws IOException if deserialization failed
	 * @throws CradleStorageException if deserialized event doesn't match batch conditions
	 */
	public static void deserializeTestEvents(byte[] contentBytes, StoredTestEventBatch batch,
			Map<StoredTestEventId, Collection<StoredMessageId>> ids)
			throws IOException, CradleStorageException
	{
		SerializationConsumer<BatchedStoredTestEvent> action = it -> {
			if (ids == null)
				StoredTestEventBatch.addTestEvent(it, batch);
			else
				StoredTestEventBatch.addTestEvent(it, batch, ids.get(it.getId()));
		};

		try {
			deserializeTestEvents(contentBytes, action);
		} catch (Exception e) {
			if (e instanceof IOException) {
				throw (IOException) e;
			} else {
				throw new IOException(e);
			}
		}
		
	}

	public static void deserializeTestEvents(byte[] contentBytes, 
			SerializationConsumer<BatchedStoredTestEvent> action) throws Exception
	{
		if (deserializer.checkEventBatchHeader(contentBytes)) {
			deserializer.deserializeBatchEntries(contentBytes, action);
		} else {
			//This code is for backward compatibility. 

			LegacyEventDeserializer.deserializeTestEvents(contentBytes, action);
		}
	}

	/**
	 * Deserializes all test events metadata, adding them to given batch for metadata
	 * @param contentBytes to deserialize events metadata from
	 * @param batch to add events to
	 * @throws IOException if deserialization failed
	 */
	public static void deserializeTestEventsMetadata(byte[] contentBytes, StoredTestEventBatchMetadata batch)
			throws IOException
	{
		deserializeTestEventsMetadata(contentBytes, batch.getId().toString(), 
				it -> StoredTestEventBatchMetadata.addTestEventMetadata(it, batch) );
	}

	/**
	 * Deserializes all test events metadata, adding them to given batch for metadata
	 * @param contentBytes to deserialize events metadata from
	 * @param batchId batchId
	 * @param action action under test metadata  
	 * @throws IOException if deserialization failed
	 */
	public static void deserializeTestEventsMetadata(byte[] contentBytes, String batchId,
		 	SerializationConsumer<BatchedStoredTestEventMetadata> action) throws IOException
	{
		try
		{
			contentBytes = CompressionUtils.decompressData(contentBytes);
		}
		catch (IOException e)
		{
			throw new IOException("Could not decompress metadata of test events from batch with ID '"+batchId+"'", e);
		}
		catch (DataFormatException e)
		{
			//Data seems to be not compressed, i.e written by Cradle API prior to 2.9.0, let's try to deserialize events from bytes as they are
		}
		
		try {
			if (deserializer.checkEventBatchMetadataHeader(contentBytes)) {
				deserializer.deserializeBatchEntriesMetadata(contentBytes, action);
			} else {
				//This code is for backward compatibility. 

				LegacyEventDeserializer.deserializeTestEventsMetadata(contentBytes, action);
			}
		} catch (Exception e) {
			if (e instanceof IOException) {
				throw (IOException) e;
			} else {
				throw new IOException(e);
			}
		}
	}
	
	
	/**
	 * Decompresses given ByteBuffer and deserializes all test events, adding them to given batch
	 * @param content to deserialize events from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @param batch to add events to
	 * @param ids Map of Collections of message IDs related to added events
	 * @throws IOException if deserialization failed
	 * @throws CradleStorageException if deserialized event doesn't match batch conditions
	 */
	public static void 	bytesToTestEvents(ByteBuffer content, boolean compressed, StoredTestEventBatch batch,
			Map<StoredTestEventId, Collection<StoredMessageId>> ids)
			throws IOException, CradleStorageException
	{
		byte[] contentBytes = getTestEventContentBytes(content, compressed, batch.getId());
		deserializeTestEvents(contentBytes, batch, ids);
	}
	
	public static byte[] getTestEventContentBytes(ByteBuffer content, boolean compressed, StoredTestEventId eventId) throws IOException
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
			throw new IOException(String.format("Could not decompress content of test event (ID: '%s') from Cradle", eventId), e);
		}
	}
	


	public static Collection<StoredMessageId> deserializeLinkedMessageIds(byte[] array) throws IOException {
		return EventMessageIdDeserializer.deserializeLinkedMessageIds(array);
	}

	public static byte[] serializeLinkedMessageIds(Collection<StoredMessageId> messageIds) throws IOException {
		return EventMessageIdSerializer.serializeLinkedMessageIds(messageIds);
	}

	public static Map<StoredTestEventId, Collection<StoredMessageId>> deserializeBatchLinkedMessageIds(byte[] array) throws IOException {
		return EventMessageIdDeserializer.deserializeBatchLinkedMessageIds(array);
	}

	public static byte[] serializeBatchLinkedMessageIds(Map<StoredTestEventId, Collection<StoredMessageId>> messageIdsMap) throws IOException {
		return EventMessageIdSerializer.serializeBatchLinkedMessageIds(messageIdsMap);
	}
}
