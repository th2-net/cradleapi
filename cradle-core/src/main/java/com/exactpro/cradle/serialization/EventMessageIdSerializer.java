/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleSerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.*;

public class EventMessageIdSerializer {


	public static byte[] serializeLinkedMessageIds(Collection<StoredMessageId> ids)
			throws IOException {
		if (ids == null || ids.isEmpty())
			return null;

		byte[] result;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 DataOutputStream dos = new DataOutputStream(baos)) {
			writeIdsStart(ids, dos);

			if (ids.size() == 1) {
				StoredMessageId id = ids.iterator().next();
				CradleSerializationUtils.writeString(id.getStreamName(), dos);
				dos.writeByte(id.getDirection() == Direction.FIRST ? DIRECTION_FIRST: DIRECTION_SECOND);
				dos.writeLong(id.getIndex());
			} else {
				Map<String, Pair<List<Long>, List<Long>>> byStream = divideIdsByStream(ids);
				for (Map.Entry<String, Pair<List<Long>, List<Long>>> streamIds : byStream.entrySet()) {
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
			throws IOException {
		if (ids == null || ids.isEmpty())
			return null;

		byte[] result;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 DataOutputStream dos = new DataOutputStream(baos)) {
			writeIdsStart(ids, dos);

			Map<String, Integer> mapping = getStreams(ids);
			writeMapping(mapping, dos);
			for (Map.Entry<StoredTestEventId, Collection<StoredMessageId>> eventMessages : ids.entrySet()) {
				CradleSerializationUtils.writeString(eventMessages.getKey().getId(), dos);
				dos.writeInt(eventMessages.getValue().size());

				Map<String, Pair<List<Long>, List<Long>>> byStream = divideIdsByStream(eventMessages.getValue());
				for (Map.Entry<String, Pair<List<Long>, List<Long>>> streamIds : byStream.entrySet()) {
					dos.writeShort(mapping.get(streamIds.getKey()));
					writeDirectionIds(streamIds.getValue(), dos);
				}
			}

			dos.flush();
			result = baos.toByteArray();
		}
		return result;
	}

	private static void writeIdsStart(Collection<StoredMessageId> ids, DataOutputStream dos) throws IOException {
		dos.writeByte(VERSION);
		dos.writeByte(SINGLE_EVENT_LINKS);
		dos.writeInt(ids.size());
	}

	private static void writeIdsStart(Map<StoredTestEventId, Collection<StoredMessageId>> ids, DataOutputStream dos) throws IOException {
		dos.writeByte(VERSION);
		dos.writeByte(BATCH_LINKS);
		dos.writeInt(ids.size());
	}

	private static void writeDirectionIds(Pair<List<Long>, List<Long>> firstSecondIds, DataOutputStream dos) throws IOException {
		List<Long> first = firstSecondIds.getLeft(),
				second = firstSecondIds.getRight();
		if (first != null && first.size() > 0)
			writeDirectionIds(Direction.FIRST, first, dos);
		if (second != null && second.size() > 0)
			writeDirectionIds(Direction.SECOND, second, dos);
		dos.writeByte(END_OF_DATA);
	}

	private static void writeDirectionIds(Direction direction, List<Long> ids, DataOutputStream dos) throws IOException {
		dos.writeByte(direction == Direction.FIRST ? DIRECTION_FIRST : DIRECTION_SECOND);
		dos.writeInt(ids.size());

		long start = -1,
				prevId = -1;
		for (long id : ids) {
			if (id < 0) {
				throw new IllegalArgumentException("prohibited sequence " + id + " for direction " + direction);
			}
			if (start < 0) {
				start = id;
				prevId = id;
				continue;
			}

			if (id != prevId + 1) {
				writeIds(start, prevId, dos);

				start = id;
				prevId = id;
			} else
				prevId = id;
		}

		if (start > -1)
			writeIds(start, prevId, dos);
	}

	private static void writeIds(long start, long end, DataOutputStream dos) throws IOException {
		if (start == end) {
			dos.writeByte(SINGLE_ID);
			dos.writeLong(start);
		} else {
			dos.writeByte(RANGE_OF_IDS);
			dos.writeLong(start);
			dos.writeLong(end);
		}
	}

	private static Map<String, Pair<List<Long>, List<Long>>> divideIdsByStream(Collection<StoredMessageId> ids) {
		int inititalCapacity = ids.size() / 2;
		Map<String, Pair<List<Long>, List<Long>>> result = new HashMap<>();
		for (StoredMessageId id : ids) {
			Pair<List<Long>, List<Long>> storage = result.computeIfAbsent(id.getStreamName(),
					sn -> new ImmutablePair<>(new ArrayList<Long>(inititalCapacity), new ArrayList<Long>(inititalCapacity)));
			if (id.getDirection() == Direction.FIRST)
				storage.getLeft().add(id.getIndex());
			else
				storage.getRight().add(id.getIndex());
		}

		for (Pair<List<Long>, List<Long>> streamIds : result.values()) {
			Collections.sort(streamIds.getLeft());
			Collections.sort(streamIds.getRight());
		}

		return result;
	}

	private static Map<String, Integer> getStreams(Map<StoredTestEventId, Collection<StoredMessageId>> ids) {
		Set<String> streams = new HashSet<>();
		ids.values().forEach(eventIds -> eventIds.forEach(id -> streams.add(id.getStreamName())));

		Map<String, Integer> result = new HashMap<>();
		for (String stream : streams)
			result.put(stream, result.size());
		return result;
	}

	private static void writeMapping(Map<String, Integer> mapping, DataOutputStream dos) throws IOException {
		dos.writeShort(mapping.size());
		for (Map.Entry<String, Integer> m : mapping.entrySet()) {
			CradleSerializationUtils.writeString(m.getKey(), dos);
			dos.writeShort(m.getValue());
		}
	}

}
