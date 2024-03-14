/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.serialization;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleSerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.*;
import static com.exactpro.cradle.serialization.Serialization.NOT_SUPPORTED_PROTOCOL_FORMAT;

public class EventMessageIdDeserializer {

	public static Set<StoredMessageId> deserializeLinkedMessageIds(byte[] bytes, BookId bookId) throws IOException
	{
		if (bytes == null || bytes.length == 0)
			return null;

		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			 DataInputStream dis = new DataInputStream(bais))
		{
			byte version = dis.readByte();
			if (version != VERSION_1)
				throw new SerializationException(String.format(NOT_SUPPORTED_PROTOCOL_FORMAT, "linkedMessageIds",
                        VERSION_1, version));
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
			if (version != VERSION_1)
				throw new SerializationException(String.format(NOT_SUPPORTED_PROTOCOL_FORMAT, "batchLinkedMessages",
                        VERSION_1, version));
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
