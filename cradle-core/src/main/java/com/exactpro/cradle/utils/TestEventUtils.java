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
import com.exactpro.cradle.testevents.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class TestEventUtils
{
	private static final byte VERSION = 1,
			SINGLE_EVENT_LINKS = 1,
			BATCH_LINKS = 2,
			END_OF_DATA = 0,
			DIRECTION_FIRST = 1,
			DIRECTION_SECOND = 2,
			SINGLE_ID = 1,
			RANGE_OF_IDS = 2;
	
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

	public static byte[] serializeLinkedMessageIds(Collection<StoredMessageId> ids)
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
				CradleSerializationUtils.writeString(id.getStreamName(), dos);
				dos.writeByte(id.getDirection() == Direction.FIRST ? DIRECTION_FIRST: DIRECTION_SECOND);
				dos.writeLong(id.getIndex());
			}
			else
			{
				Map<String, Pair<List<Long>, List<Long>>> byStream = divideIdsByStream(ids);
				for (Entry<String, Pair<List<Long>, List<Long>>> streamIds : byStream.entrySet())
				{
					CradleSerializationUtils.writeString(streamIds.getKey(), dos);
					writeDirectionIds(streamIds.getValue(), dos);
				}
			}
			
			dos.flush();
			result = baos.toByteArray();
		}
		return result;
	}
	
	public static byte[] serializeBatchLinkedMessageIds(Map<StoredTestEventId, Collection<StoredMessageId>> ids)
			throws IOException
	{
		if (ids == null || ids.isEmpty())
			return null;
		
		byte[] result;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos))
		{
			writeIdsStart(ids, dos);
			
			Map<String, Integer> mapping = getStreams(ids);
			writeMapping(mapping, dos);
			for (Entry<StoredTestEventId, Collection<StoredMessageId>> eventMessages : ids.entrySet())
			{
				CradleSerializationUtils.writeString(eventMessages.getKey().getId(), dos);
				dos.writeInt(eventMessages.getValue().size());
				
				Map<String, Pair<List<Long>, List<Long>>> byStream = divideIdsByStream(eventMessages.getValue());
				for (Entry<String, Pair<List<Long>, List<Long>>> streamIds : byStream.entrySet())
				{
					dos.writeShort(mapping.get(streamIds.getKey()));
					writeDirectionIds(streamIds.getValue(), dos);
				}
			}
			
			dos.flush();
			result = baos.toByteArray();
		}
		return result;
	}

	public static Collection<StoredMessageId> deserializeLinkedMessageIds(byte[] bytes) throws IOException
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
			Collection<StoredMessageId> result = new ArrayList<>(size);
			if (size == 1)
			{
				String streamName = CradleSerializationUtils.readString(dis);
				Direction direction = readDirection(dis);
				if (direction == null)
					throw new IOException("Invalid direction");
				result.add(new StoredMessageId(streamName, direction, dis.readLong()));
				return result;
			}
			
			while (result.size() < size)
			{
				String streamName = CradleSerializationUtils.readString(dis);
				readDirectionIds(streamName, result, dis);
			}
			return result;
		}
	}
	
	public static Map<StoredTestEventId, Collection<StoredMessageId>> deserializeBatchLinkedMessageIds(byte[] bytes) throws IOException
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
			Map<StoredTestEventId, Collection<StoredMessageId>> result = new HashMap<>(eventsTotal);
			
			Map<Integer, String> mapping = readMapping(dis);
			
			while (result.size() < eventsTotal)
			{
				StoredTestEventId eventId = new StoredTestEventId(CradleSerializationUtils.readString(dis));
				int size = dis.readInt();
				Collection<StoredMessageId> eventLinks = new ArrayList<>(size);
				
				while (eventLinks.size() < size)
				{
					int index = dis.readShort();
					String streamName = mapping.get(index);
					readDirectionIds(streamName, eventLinks, dis);
				}
				
				result.put(eventId, eventLinks);
			}
			return result;
		}
	}

	/**
	 * Serializes test events metadata, skipping non-meaningful or calculatable fields
	 * @param testEventsMetadata to serialize
	 * @return array of bytes, containing serialized and compressed metadata of events
	 * @throws IOException if serialization failed
	 */
	public static byte[] serializeTestEventsMetadata(Collection<BatchedStoredTestEventMetadata> testEventsMetadata) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(out))
		{
			for (BatchedStoredTestEventMetadata te : testEventsMetadata)
				serialize(te, dos);
			dos.flush();
			batchContent = CompressionUtils.compressData(out.toByteArray());
		}
		return batchContent;
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
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] teBytes = readNextData(dis);
				BatchedStoredTestEvent tempTe = deserializeTestEvent(teBytes);
				//Workaround to fix events stored before commit f71b224e6f4dc0c8c99512de6a8f2034a1c3badc. TODO: remove it in future
				if (tempTe.getParentId() == null)
				{
					TestEventToStore te = TestEventToStore.builder()
							.id(tempTe.getId())
							.name(tempTe.getName())
							.type(tempTe.getType())
							.parentId(batch.getParentId())
							.startTimestamp(tempTe.getStartTimestamp())
							.endTimestamp(tempTe.getEndTimestamp())
							.success(tempTe.isSuccess())
							.content(tempTe.getContent())
							.build();
					if (ids == null)
						StoredTestEventBatch.addTestEvent(te, batch);
					else
						StoredTestEventBatch.addTestEvent(te, batch, ids.get(te.getId()));
				}
				else
				{
					if (ids == null)
						StoredTestEventBatch.addTestEvent(tempTe, batch);
					else
						StoredTestEventBatch.addTestEvent(tempTe, batch, ids.get(tempTe.getId()));
				}
			}
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
		try
		{
			contentBytes = CompressionUtils.decompressData(contentBytes);
		}
		catch (IOException e)
		{
			throw new IOException("Could not decompress metadata of test events from batch with ID '"+batch.getId()+"'", e);
		}
		catch (DataFormatException e)
		{
			//Data seems to be not compressed, i.e written by Cradle API prior to 2.9.0, let's try to deserialize events from bytes as they are
		}
		
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] teBytes = readNextData(dis);
				BatchedStoredTestEventMetadata tempTe = deserializeTestEventMetadata(teBytes);
				StoredTestEventBatchMetadata.addTestEventMetadata(tempTe, batch);
			}
		}
	}
	
	
	/**
	 * Decompresses given ByteBuffer and deserializes all test events, adding them to given batch
	 * @param content to deserialize events from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @param batch to add events to
	 * @param ids Map of Collection of messages' id's related with added events
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
	
	private static BatchedStoredTestEventMetadata deserializeTestEventMetadata(byte[] bytes)
	{
		return (BatchedStoredTestEventMetadata)SerializationUtils.deserialize(bytes);
	}
	
	
	private static void writeIdsStart(Collection<StoredMessageId> ids, DataOutputStream dos) throws IOException
	{
		dos.writeByte(VERSION);
		dos.writeByte(SINGLE_EVENT_LINKS);
		dos.writeInt(ids.size());
	}
	
	private static void writeIdsStart(Map<StoredTestEventId, Collection<StoredMessageId>> ids, DataOutputStream dos) throws IOException
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
	
	private static Map<String, Pair<List<Long>, List<Long>>> divideIdsByStream(Collection<StoredMessageId> ids)
	{
		int inititalCapacity = ids.size() / 2;
		Map<String, Pair<List<Long>, List<Long>>> result = new HashMap<>();
		for (StoredMessageId id : ids)
		{
			Pair<List<Long>, List<Long>> storage = result.computeIfAbsent(id.getStreamName(), 
					sn -> new ImmutablePair<>(new ArrayList<Long>(inititalCapacity), new ArrayList<Long>(inititalCapacity)));
			if (id.getDirection() == Direction.FIRST)
				storage.getLeft().add(id.getIndex());
			else
				storage.getRight().add(id.getIndex());
		}
		
		for (Pair<List<Long>, List<Long>> streamIds : result.values())
		{
			Collections.sort(streamIds.getLeft());
			Collections.sort(streamIds.getRight());
		}
		
		return result;
	}
	
	private static Map<String, Integer> getStreams(Map<StoredTestEventId, Collection<StoredMessageId>> ids)
	{
		Set<String> streams = new HashSet<>();
		ids.values().forEach(eventIds -> eventIds.forEach(id -> streams.add(id.getStreamName())));
		
		Map<String, Integer> result = new HashMap<>();
		for (String stream : streams)
			result.put(stream, result.size());
		return result;
	}
	
	private static void writeDirectionIds(Pair<List<Long>, List<Long>> firstSecondIds, DataOutputStream dos) throws IOException
	{
		List<Long> first = firstSecondIds.getLeft(),
				second = firstSecondIds.getRight();
		if (first != null && first.size() > 0)
			writeDirectionIds(Direction.FIRST, first, dos);
		if (second != null && second.size() > 0)
			writeDirectionIds(Direction.SECOND, second, dos);
		dos.writeByte(END_OF_DATA);
	}
	
	private static void writeDirectionIds(Direction direction, List<Long> ids, DataOutputStream dos) throws IOException
	{
		dos.writeByte(direction == Direction.FIRST ? DIRECTION_FIRST : DIRECTION_SECOND);
		dos.writeInt(ids.size());
		
		long start = -1,
				prevId = -1;
		for (long id : ids)
		{
			if (start < 0)
			{
				start = id;
				prevId = id;
				continue;
			}
			
			if (id != prevId+1)
			{
				writeIds(start, prevId, dos);
				
				start = id;
				prevId = id;
			}
			else
				prevId = id;
		}
		
		if (start > -1)
			writeIds(start, prevId, dos);
	}
	
	private static void writeIds(long start, long end, DataOutputStream dos) throws IOException
	{
		if (start == end)
		{
			dos.writeByte(SINGLE_ID);
			dos.writeLong(start);
		}
		else
		{
			dos.writeByte(RANGE_OF_IDS);
			dos.writeLong(start);
			dos.writeLong(end);
		}
	}
	
	
	private static Map<Integer, String> readMapping(DataInputStream dis) throws IOException
	{
		int size = dis.readShort();
		Map<Integer, String> result = new HashMap<>(size);
		for (int i = 0; i < size; i++)
		{
			String streamName = CradleSerializationUtils.readString(dis);
			int index = dis.readShort();
			result.put(index, streamName);
		}
		return result;
	}
	
	private static void readDirectionIds(String streamName, Collection<StoredMessageId> result, DataInputStream dis) throws IOException
	{
		Direction direction;
		while ((direction = readDirection(dis)) != null)
			readDirectionIds(direction, streamName, result, dis);
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
	
	private static void readDirectionIds(Direction direction, String streamName, 
			Collection<StoredMessageId> result, DataInputStream dis) throws IOException
	{
		int size = dis.readInt();
		int count = 0;
		while (count < size)
		{
			byte mark = dis.readByte();
			if (mark == SINGLE_ID)
			{
				result.add(new StoredMessageId(streamName, direction, dis.readLong()));
				count++;
			}
			else
			{
				long start = dis.readLong(),
						end = dis.readLong();
				for (long i = start; i <= end; i++)
				{
					result.add(new StoredMessageId(streamName, direction, i));
					count++;
				}
			}
		}
	}
}
