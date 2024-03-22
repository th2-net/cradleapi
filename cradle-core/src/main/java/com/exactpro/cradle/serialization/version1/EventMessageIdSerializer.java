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

package com.exactpro.cradle.serialization.version1;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.utils.CradleSerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.*;

public class EventMessageIdSerializer {

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
				for (Map.Entry<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> sessionIds : bySession.entrySet())
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

	public static byte[] serializeBatchLinkedMessageIds(Collection<TestEventSingleToStore> eventsWithAttachedMessages)
			throws IOException
	{
		if (eventsWithAttachedMessages == null || eventsWithAttachedMessages.isEmpty()) {
            return null;
        }

		byte[] result;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 DataOutputStream dos = new DataOutputStream(baos))
		{
			writeIdsStart(eventsWithAttachedMessages, dos);

			Map<String, Integer> mapping = getSessions(eventsWithAttachedMessages);
			writeMapping(mapping, dos);
			for (TestEventSingleToStore eventMessages : eventsWithAttachedMessages)
			{
				StoredTestEventId eventId = eventMessages.getId();
				CradleSerializationUtils.writeString(eventId.getScope(), dos);
				CradleSerializationUtils.writeInstant(eventId.getStartTimestamp(), dos);
				CradleSerializationUtils.writeString(eventId.getId(), dos);

				dos.writeInt(eventMessages.getMessages().size());

				Map<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> bySession = divideIdsBySession(eventMessages.getMessages());
				for (Map.Entry<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> sessionIds : bySession.entrySet())
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

	private static void writeIdsStart(Set<StoredMessageId> ids, DataOutputStream dos) throws IOException
	{
		dos.writeByte(VERSION_1);
		dos.writeByte(SINGLE_EVENT_LINKS);
		dos.writeInt(ids.size());
	}

	private static void writeIdsStart(Collection<TestEventSingleToStore> eventsWithAttachedMessages, DataOutputStream dos) throws IOException
	{
		dos.writeByte(VERSION_1);
		dos.writeByte(BATCH_LINKS);
		dos.writeInt(eventsWithAttachedMessages.size());
	}

	private static void writeMapping(Map<String, Integer> mapping, DataOutputStream dos) throws IOException
	{
		dos.writeShort(mapping.size());
		for (Map.Entry<String, Integer> m : mapping.entrySet())
		{
			CradleSerializationUtils.writeString(m.getKey(), dos);
			dos.writeShort(m.getValue());
		}
	}

	private static Map<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> divideIdsBySession(Set<StoredMessageId> ids)
	{
		int initialCapacity = ids.size() / 2;
		Map<String, Pair<List<StoredMessageId>, List<StoredMessageId>>> result = new HashMap<>();
		for (StoredMessageId id : ids)
		{
			Pair<List<StoredMessageId>, List<StoredMessageId>> storage = result.computeIfAbsent(id.getSessionAlias(),
					sn -> new ImmutablePair<>(new ArrayList<>(initialCapacity), new ArrayList<>(initialCapacity)));
			if (id.getDirection() == Direction.FIRST)
				storage.getLeft().add(id);
			else
				storage.getRight().add(id);
		}

		return result;
	}

	private static Map<String, Integer> getSessions(Collection<TestEventSingleToStore> eventsWithAttachedMessages)
	{
		Set<String> sessions = new HashSet<>();
		eventsWithAttachedMessages.forEach(event -> event.getMessages().forEach(id -> sessions.add(id.getSessionAlias())));

		Map<String, Integer> result = new HashMap<>();
		for (String session : sessions)
			result.put(session, result.size());
		return result;
	}

	private static void writeDirectionIds(Pair<List<StoredMessageId>, List<StoredMessageId>> firstSecondIds, DataOutputStream dos) throws IOException
	{
		List<StoredMessageId> first = firstSecondIds.getLeft(),
				second = firstSecondIds.getRight();
		if (first != null && !first.isEmpty())
			writeDirectionIds(Direction.FIRST, first, dos);
		if (second != null && !second.isEmpty())
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

	
}
