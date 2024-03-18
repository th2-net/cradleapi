/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.DIRECTION_FIRST;
import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.DIRECTION_SECOND;
import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.SINGLE_EVENT_LINKS;
import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.VERSION_2;
import static com.exactpro.cradle.utils.CradleSerializationUtils.writeInstant;
import static com.exactpro.cradle.utils.CradleSerializationUtils.writeString;

public class EventMessageIdSerializer2 {

	public static ByteBuffer serializeLinkedMessageIds(Set<StoredMessageId> msgIds) {
		if (msgIds == null || msgIds.isEmpty()) {
			return null;
		}

		if (msgIds.size() == 1) {
			StoredMessageId msgId = msgIds.iterator().next();
			// 1B: version, 1B: event type, 2B: number of events
			// 2B + nB: session alias length in bytes, 1B: direction, 12B: timestamp, 8B: sequence
			int size = 27 + msgId.getSessionAlias().getBytes().length;

			ByteBuffer buffer = ByteBuffer.allocate(size);
			writeIdsStart((short) msgIds.size(), buffer);
			writeString(msgId.getSessionAlias(), buffer);
			buffer.put(msgId.getDirection() == Direction.FIRST ? DIRECTION_FIRST : DIRECTION_SECOND);
			writeInstant(msgId.getTimestamp(), buffer);
			buffer.putLong(msgId.getSequence());
			return buffer;
		} else {
			Map<String, Short> aliasMapping = new HashMap<>();
			// 1B: version, 1B: event type, 2B: number of events, 2B: mapping size
			final Counter size = new Counter(6);
			final Counter counter = new Counter();
			for (StoredMessageId msgId : msgIds) {
				aliasMapping.computeIfAbsent(msgId.getSessionAlias(), alias -> {
					// 2B + nB: session alias length in bytes, 2B: short number
					size.inc(4 + alias.getBytes().length);
					return (short) counter.inc();
				});
				// 2B: session alias short number, 1B: direction, 12B: timestamp, 8B: sequence
				size.inc(23);
			}

			ByteBuffer buffer = ByteBuffer.allocate(size.num);
			writeIdsStart((short) msgIds.size(), buffer);
			writeMapping(aliasMapping, buffer);
			for (StoredMessageId msgId : msgIds) {
				buffer.putShort(aliasMapping.get(msgId.getSessionAlias()));
				buffer.put(msgId.getDirection() == Direction.FIRST ? DIRECTION_FIRST : DIRECTION_SECOND);
				writeInstant(msgId.getTimestamp(), buffer);
				buffer.putLong(msgId.getSequence());
			}
			return buffer;
		}
	}

	public static ByteBuffer serializeBatchLinkedMessageIds(Collection<TestEventSingleToStore> eventsWithAttachedMessages) {
		if (eventsWithAttachedMessages.isEmpty()) {
			return null;
		}

		Map<String, Short> aliasMapping = new HashMap<>();
		// 1B: version, 1B: event type, 2B: number of events, 2B: mapping size
		final Counter size = new Counter(6);
		Counter counter = new Counter();
		for (TestEventSingleToStore event : eventsWithAttachedMessages) {
			if (!event.hasMessages()) {
				continue;
			}
			StoredTestEventId eventId = event.getId();
			Set<StoredMessageId> msgIds = event.getMessages();
			// 12B: timestamp, 2B + nB: id length in bytes, 2B: number of message ids
			size.inc(16 + eventId.getId().getBytes().length);

			for (StoredMessageId msgId : msgIds) {
				aliasMapping.computeIfAbsent(msgId.getSessionAlias(), alias -> {
					// 2B + nB: session alias length in bytes, 2B: short number
					size.inc(4 + alias.getBytes().length);
					return (short) counter.inc();
				});
				// 2B: session alias short number, 1B: direction, 12B: timestamp, 8B: sequence
				size.inc(23);
			}
		}

		ByteBuffer buffer = ByteBuffer.allocate(size.num);
		writeIdsStart((short) eventsWithAttachedMessages.size(), buffer);
		writeMapping(aliasMapping, buffer);
		for (TestEventSingleToStore event : eventsWithAttachedMessages) {
			StoredTestEventId eventId = event.getId();
			Set<StoredMessageId> msgIds = event.getMessages();

			writeInstant(eventId.getStartTimestamp(), buffer);
			writeString(eventId.getId(), buffer);
			buffer.putShort((short) msgIds.size());

			for (StoredMessageId msgId : msgIds) {
				buffer.putShort(aliasMapping.get(msgId.getSessionAlias()));
				buffer.put(msgId.getDirection() == Direction.FIRST ? DIRECTION_FIRST : DIRECTION_SECOND);
				writeInstant(msgId.getTimestamp(), buffer);
				buffer.putLong(msgId.getSequence());
			}
		}
		return buffer;
	}

	private static void writeIdsStart(short size, ByteBuffer buffer) {
		buffer.put(VERSION_2);
		buffer.put(SINGLE_EVENT_LINKS);
		buffer.putShort(size);
	}

	private static void writeMapping(Map<String, Short> mapping, ByteBuffer buffer) {
		buffer.putShort((short) mapping.size());
		for (Map.Entry<String, Short> entry : mapping.entrySet()) {
			writeString(entry.getKey(), buffer);
			buffer.putShort(entry.getValue());
		}
	}

	private static class Counter {
		private int num;

		public Counter(int num) {
			this.num = num;
		}
		public Counter() {
			this(0);
		}
		public int inc(int num) {
			this.num += num;
			return this.num;
		}

		public int inc() {
			return inc(1);
		}

		@Override
		public String toString() {
			return String.valueOf(num);
		}
	}
}
