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

package com.exactpro.cradle.serialization.version2;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.serialization.SerializationException;
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.BATCH_LINKS;
import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.DIRECTION_FIRST;
import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.DIRECTION_SECOND;
import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.SINGLE_EVENT_LINKS;
import static com.exactpro.cradle.serialization.Serialization.EventMessageIdsConst.VERSION_2;
import static com.exactpro.cradle.serialization.Serialization.NOT_SUPPORTED_PROTOCOL_FORMAT;
import static com.exactpro.cradle.utils.CradleSerializationUtils.readInstant;
import static com.exactpro.cradle.utils.CradleSerializationUtils.readString;

public class EventMessageIdDeserializer {

    public static Set<StoredMessageId> deserializeLinkedMessageIds(byte[] bytes, BookId bookId) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        byte version = buffer.get();
        if (version != VERSION_2) {
            throw new SerializationException(String.format(NOT_SUPPORTED_PROTOCOL_FORMAT, "linkedMessageIds",
                    VERSION_2, version));
        }
        byte mark = buffer.get();
        if (mark != SINGLE_EVENT_LINKS) {
            throw new SerializationException("Unexpected data mark. Expected " + SINGLE_EVENT_LINKS + ", got " + mark);
        }

        int size = buffer.getShort();
        Set<StoredMessageId> result = new HashSet<>(size);
        if (size == 1) {
            String sessionAlias = readString(buffer);
            Direction direction = readDirection(buffer);
            if (direction == null) {
                throw new SerializationException("Invalid direction");
            }
            Instant timestamp = readInstant(buffer);
            result.add(new StoredMessageId(bookId, sessionAlias, direction, timestamp, buffer.getLong()));
        } else {
            Map<Integer, String> mapping = readMapping(buffer);
            for (int i = 0; i < size; i++) {
                result.add(readMessageIds(bookId, mapping, buffer));
            }
        }
        return result;
    }

    public static Map<StoredTestEventId, Set<StoredMessageId>> deserializeBatchLinkedMessageIds(byte[] bytes,
                                                                                                BookId bookId,
                                                                                                String scope) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte version = buffer.get();
        if (version != VERSION_2) {
            throw new SerializationException(String.format(NOT_SUPPORTED_PROTOCOL_FORMAT, "batchLinkedMessages",
                    VERSION_2, version));
        }
        byte mark = buffer.get();
        if (mark != BATCH_LINKS) {
            throw new SerializationException("Unexpected data mark. Expected " + BATCH_LINKS + ", got " + mark);
        }

        int events = buffer.getShort();
        Map<StoredTestEventId, Set<StoredMessageId>> result = new HashMap<>(events);

        Map<Integer, String> mapping = readMapping(buffer);

        for (int i = 0; i < events; i++) {
            Instant startTimestamp = readInstant(buffer);
            String id = readString(buffer);
            StoredTestEventId eventId = new StoredTestEventId(bookId, scope, startTimestamp, id);

            int size = buffer.getShort();
            Set<StoredMessageId> eventLinks = new HashSet<>(size);

            for (int j = 0; j < size; j++) {
                eventLinks.add(readMessageIds(bookId, mapping, buffer));
            }

            result.put(eventId, eventLinks);
        }
        return result;
    }

    private static Map<Integer, String> readMapping(ByteBuffer buffer) {
        int size = buffer.getShort();
        Map<Integer, String> result = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String sessionAlias = readString(buffer);
            int index = buffer.getShort();
            result.put(index, sessionAlias);
        }
        return result;
    }

    private static StoredMessageId readMessageIds(BookId bookId,
                                                  Map<Integer, String> mapping,
                                                  ByteBuffer buffer) throws SerializationException {
        int index = buffer.getShort();
        return new StoredMessageId(bookId,
                mapping.get(index),
                readDirection(buffer),
                readInstant(buffer),
                buffer.getLong());
    }

    private static Direction readDirection(ByteBuffer buffer) throws SerializationException {
        byte direction = buffer.get();
        if (direction == 0) {
            return null;
        }
        if (direction == DIRECTION_FIRST) {
            return Direction.FIRST;
        } else if (direction == DIRECTION_SECOND) {
            return Direction.SECOND;
        }
        throw new SerializationException("Unknown direction - " + direction);
    }
}
