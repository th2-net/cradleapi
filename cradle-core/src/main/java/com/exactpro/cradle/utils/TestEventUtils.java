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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.DataFormatException;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEvent;
import com.exactpro.cradle.testevents.TestEventBatch;
import com.exactpro.cradle.testevents.TestEventSingle;
import com.exactpro.cradle.testevents.TestEventToStore;

public class TestEventUtils
{
	private static final Logger logger = LoggerFactory.getLogger(TestEventUtils.class);
	
	private static final byte VERSION = 1,
			SINGLE_EVENT_LINKS = 1,
			BATCH_LINKS = 2,
			END_OF_DATA = 0,
			DIRECTION_FIRST = 1,
			DIRECTION_SECOND = 2;
	
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
			throws IOException
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
			return serializeBatchLinkedMessageIds(event.asBatch().getBatchMessages());
		return serializeLinkedMessageIds(event.asSingle().getMessages());
	}
	
	public static byte[] serializeLinkedMessageIds(Set<StoredMessageId> ids)
			throws IOException
	{
		if (ids == null || ids.isEmpty())
			return null;
		
		byte[] result;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos))
		{
			writeIdsStart(ids, dos);
			
			if (ids.size() == 1)
			{
				StoredMessageId id = ids.iterator().next();
				CradleSerializationUtils.writeString(id.getSessionAlias(), dos);
				dos.writeByte(id.getDirection() == Direction.FIRST ? DIRECTION_FIRST: DIRECTION_SECOND);
				CradleSerializationUtils.writeInstant(id.getTimestamp(), dos);
				dos.writeLong(id.getSequence());
			}
			else
			{
				Map<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> bySession = divideIdsBySession(ids);
				for (Entry<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> sessionIds : bySession.entrySet())
				{
					CradleSerializationUtils.writeString(sessionIds.getKey(), dos);
					writeDirectionIds(sessionIds.getValue(), dos);
				}
			}
			
			dos.flush();
			result = baos.toByteArray();
		}
		return result;
	}
	
	public static byte[] serializeBatchLinkedMessageIds(Map<StoredTestEventId, Set<StoredMessageId>> ids)
			throws IOException
	{
		if (ids == null || ids.isEmpty())
			return null;
		
		byte[] result;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos))
		{
			writeIdsStart(ids, dos);
			
			Map<String, Integer> mapping = getSessions(ids);
			writeMapping(mapping, dos);
			for (Entry<StoredTestEventId, Set<StoredMessageId>> eventMessages : ids.entrySet())
			{
				StoredTestEventId eventId = eventMessages.getKey();
				CradleSerializationUtils.writeString(eventId.getScope(), dos);
				CradleSerializationUtils.writeInstant(eventId.getStartTimestamp(), dos);
				CradleSerializationUtils.writeString(eventId.getId(), dos);
				
				dos.writeInt(eventMessages.getValue().size());
				
				Map<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> bySession = divideIdsBySession(eventMessages.getValue());
				for (Entry<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> sessionIds : bySession.entrySet())
				{
					dos.writeShort(mapping.get(sessionIds.getKey()));
					writeDirectionIds(sessionIds.getValue(), dos);
				}
			}
			
			dos.flush();
			result = baos.toByteArray();
		}
		return result;
	}

	public static Set<StoredMessageId> deserializeLinkedMessageIds(byte[] bytes, BookId bookId) throws IOException
	{
		if (bytes == null || bytes.length == 0)
			return null;
		
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				DataInputStream dis = new DataInputStream(bais))
		{
			byte version = dis.readByte();
			if (version != VERSION)
				throw new IOException("Unsupported data format version - "+version);
			byte mark = dis.readByte();
			if (mark != SINGLE_EVENT_LINKS)
				throw new IOException("Unexpected data mark. Expected "+SINGLE_EVENT_LINKS+", got "+mark);
			
			int size = dis.readInt();
			Set<StoredMessageId> result = new HashSet<>(size);
			if (size == 1)
			{
				String sessionAlias = CradleSerializationUtils.readString(dis);
				Direction direction = readDirection(dis);
				if (direction == null)
					throw new IOException("Invalid direction");
				Instant timestamp = CradleSerializationUtils.readInstant(dis);
				result.add(new StoredMessageId(bookId, sessionAlias, direction, timestamp, dis.readLong()));
				return result;
			}
			
			while (result.size() < size)
			{
				String sessionAlias = CradleSerializationUtils.readString(dis);
				readDirectionIds(bookId, sessionAlias, result, dis);
			}
			return result;
		}
	}
	
	public static Map<StoredTestEventId, Set<StoredMessageId>> deserializeBatchLinkedMessageIds(byte[] bytes, BookId bookId) throws IOException
	{
		if (bytes == null || bytes.length == 0)
			return null;
		
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				DataInputStream dis = new DataInputStream(bais))
		{
			byte version = dis.readByte();
			if (version != VERSION)
				throw new IOException("Unsupported data format version - "+version);
			byte mark = dis.readByte();
			if (mark != BATCH_LINKS)
				throw new IOException("Unexpected data mark. Expected "+BATCH_LINKS+", got "+mark);
			
			int eventsTotal = dis.readInt();
			Map<StoredTestEventId, Set<StoredMessageId>> result = new HashMap<>(eventsTotal);
			
			Map<Integer, String> mapping = readMapping(dis);
			
			while (result.size() < eventsTotal)
			{
				String scope = CradleSerializationUtils.readString(dis);
				Instant startTimestamp = CradleSerializationUtils.readInstant(dis);
				String id = CradleSerializationUtils.readString(dis);
				StoredTestEventId eventId = new StoredTestEventId(bookId, scope, startTimestamp, id);
				
				int size = dis.readInt();
				Set<StoredMessageId> eventLinks = new HashSet<>(size);
				
				while (eventLinks.size() < size)
				{
					int index = dis.readShort();
					String sessionAlias = mapping.get(index);
					readDirectionIds(bookId, sessionAlias, eventLinks, dis);
				}
				
				result.put(eventId, eventLinks);
			}
			return result;
		}
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
	
	
	private static void writeIdsStart(Set<StoredMessageId> ids, DataOutputStream dos) throws IOException
	{
		dos.writeByte(VERSION);
		dos.writeByte(SINGLE_EVENT_LINKS);
		dos.writeInt(ids.size());
	}
	
	private static void writeIdsStart(Map<StoredTestEventId, Set<StoredMessageId>> ids, DataOutputStream dos) throws IOException
	{
		dos.writeByte(VERSION);
		dos.writeByte(BATCH_LINKS);
		dos.writeInt(ids.size());
	}
	
	private static void writeMapping(Map<String, Integer> mapping, DataOutputStream dos) throws IOException
	{
		dos.writeShort(mapping.size());
		for (Entry<String, Integer> m : mapping.entrySet())
		{
			CradleSerializationUtils.writeString(m.getKey(), dos);
			dos.writeShort(m.getValue());
		}
	}
	
	private static Map<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> divideIdsBySession(Set<StoredMessageId> ids)
	{
		int inititalCapacity = ids.size() / 2;
		Map<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> result = new HashMap<>();
		for (StoredMessageId id : ids)
		{
			Pair<List<StoredMessageId>, List<StoredMessageId>> storage = result.computeIfAbsent(id.getSessionAlias(), 
					sn -> new ImmutablePair<>(new ArrayList<>(inititalCapacity), new ArrayList<>(inititalCapacity)));
			if (id.getDirection() == Direction.FIRST)
				storage.getLeft().add(id);
			else
				storage.getRight().add(id);
		}
		
		return result;
	}
	
	private static Map<String, Integer> getSessions(Map<StoredTestEventId, Set<StoredMessageId>> ids)
	{
		Set<String> sessions = new HashSet<>();
		ids.values().forEach(eventIds -> eventIds.forEach(id -> sessions.add(id.getSessionAlias())));
		
		Map<String, Integer> result = new HashMap<>();
		for (String session : sessions)
			result.put(session, result.size());
		return result;
	}
	
	private static void writeDirectionIds(Pair<List<StoredMessageId>, List<StoredMessageId>> firstSecondIds, DataOutputStream dos) throws IOException
	{
		List<StoredMessageId> first = firstSecondIds.getLeft(),
				second = firstSecondIds.getRight();
		if (first != null && first.size() > 0)
			writeDirectionIds(Direction.FIRST, first, dos);
		if (second != null && second.size() > 0)
			writeDirectionIds(Direction.SECOND, second, dos);
		dos.writeByte(END_OF_DATA);
	}
	
	private static void writeDirectionIds(Direction direction, List<StoredMessageId> ids, DataOutputStream dos) throws IOException
	{
		dos.writeByte(direction == Direction.FIRST ? DIRECTION_FIRST : DIRECTION_SECOND);
		dos.writeInt(ids.size());
		
		for (StoredMessageId id : ids)
			writeId(id, dos);
	}
	
	private static void writeId(StoredMessageId id, DataOutputStream dos) throws IOException
	{
		CradleSerializationUtils.writeInstant(id.getTimestamp(), dos);
		dos.writeLong(id.getSequence());
	}
	
	
	private static Map<Integer, String> readMapping(DataInputStream dis) throws IOException
	{
		int size = dis.readShort();
		Map<Integer, String> result = new HashMap<>(size);
		for (int i = 0; i < size; i++)
		{
			String sessionAlias = CradleSerializationUtils.readString(dis);
			int index = dis.readShort();
			result.put(index, sessionAlias);
		}
		return result;
	}
	
	private static void readDirectionIds(BookId bookId, String sessionAlias, Collection<StoredMessageId> result, DataInputStream dis) throws IOException
	{
		Direction direction;
		while ((direction = readDirection(dis)) != null)
			readDirectionIds(direction, sessionAlias, bookId, result, dis);
	}
	
	private static Direction readDirection(DataInputStream dis) throws IOException
	{
		byte direction = dis.readByte();
		if (direction == 0)
			return null;
		if (direction == DIRECTION_FIRST)
			return Direction.FIRST;
		else if (direction == DIRECTION_SECOND)
			return Direction.SECOND;
		throw new IOException("Unknown direction - "+direction);
	}
	
	private static void readDirectionIds(Direction direction, String sessionAlias, BookId bookId,
			Collection<StoredMessageId> result, DataInputStream dis) throws IOException
	{
		int size = dis.readInt();
		int count = 0;
		while (count < size)
		{
			result.add(new StoredMessageId(bookId, sessionAlias, direction, CradleSerializationUtils.readInstant(dis), dis.readLong()));
			count++;
		}
	}
}
