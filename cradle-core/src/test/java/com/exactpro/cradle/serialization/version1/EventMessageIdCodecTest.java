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

package com.exactpro.cradle.serialization.version1;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import org.apache.commons.codec.DecoderException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.exactpro.cradle.CoreStorageSettings.DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;
import static com.exactpro.cradle.Direction.FIRST;
import static com.exactpro.cradle.Direction.SECOND;
import static com.exactpro.cradle.serialization.version1.EventMessageIdDeserializer.deserializeBatchLinkedMessageIds;
import static com.exactpro.cradle.serialization.version1.EventMessageIdDeserializer.deserializeLinkedMessageIds;
import static com.exactpro.cradle.serialization.version1.EventMessageIdSerializer.serializeBatchLinkedMessageIds;
import static com.exactpro.cradle.serialization.version1.EventMessageIdSerializer.serializeLinkedMessageIds;
import static org.apache.commons.codec.binary.Hex.decodeHex;
import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class EventMessageIdCodecTest {

    private static final BookId BOOK_ID = new BookId("test-book");
    private static final String SCOPE = "test-scope";
    public static final String SERIALIZED_EVENTS = "01020000000200020014746573742D73657373696F6E2D616C6961732D3100000" +
            "014746573742D73657373696F6E2D616C6961732D320001000A746573742D73636F70650000000065F0EC8000000000000974657" +
            "3742D69642D3100000002000001000000010000000065F0EC8000000000000000000000000102000000010000000065F0EC80000" +
            "00000000000000000000200000A746573742D73636F70650000000065F0EC80000000000009746573742D69642D3200000002000" +
            "101000000010000000065F0EC8000000000000000000000000302000000010000000065F0EC8000000000000000000000000400";
    public static final String SERIALIZED_MESSAGE_IDS = "0101000000040014746573742d73657373696f6e2d616c6961732d310100" +
            "0000010000000065f0ec8000000000000000000000000102000000010000000065f0ec8000000000000000000000000200001474" +
            "6573742d73657373696f6e2d616c6961732d3201000000010000000065f0ec800000000000000000000000030200000001000000" +
            "0065f0ec8000000000000000000000000400";
    public static final String SERIALIZED_MESSAGE_ID = "0101000000010014746573742d73657373696f6e2d616c6961732d3101000" +
            "0000065f0ec80000000000000000000000001";

    @Test
    public void testSerializeBatchLinkedMessageIds() throws CradleStorageException, IOException {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        List<TestEventSingleToStore> source = List.of(
                TestEventSingleToStore.builder(DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS)
                        .id(BOOK_ID, SCOPE, timestamp, "test-id-1")
                        .name("test-event")
                        .message(new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1))
                        .message(new StoredMessageId(BOOK_ID, "test-session-alias-1", SECOND, timestamp, 2))
                        .build(),
                TestEventSingleToStore.builder(DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS)
                        .id(BOOK_ID, SCOPE, timestamp, "test-id-2")
                        .name("test-event")
                        .message(new StoredMessageId(BOOK_ID, "test-session-alias-2", FIRST, timestamp, 3))
                        .message(new StoredMessageId(BOOK_ID, "test-session-alias-2", SECOND, timestamp, 4))
                        .build()
        );

        byte[] bytes = serializeBatchLinkedMessageIds(source);
        assertNotNull(bytes);
        // Result can't be checked because TestEventSingleToStore class uses hash set to hold StoredMessageId
        assertThat(deserializeBatchLinkedMessageIds(bytes, BOOK_ID)).usingRecursiveComparison()
                .isEqualTo(source.stream().collect(Collectors.toMap(
                        TestEventSingleToStore::getId,
                        TestEventSingleToStore::getMessages
                )));

    }

    @Test
    public void testDeserializeBatchLinkedMessageIds() throws DecoderException, IOException {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Map<StoredTestEventId, Set<StoredMessageId>> target = Map.of(
                new StoredTestEventId(BOOK_ID, SCOPE, timestamp, "test-id-1"), Set.of(
                        new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1),
                        new StoredMessageId(BOOK_ID, "test-session-alias-1", SECOND, timestamp, 2)
                ),
                new StoredTestEventId(BOOK_ID, SCOPE, timestamp, "test-id-2"), Set.of(
                        new StoredMessageId(BOOK_ID, "test-session-alias-2", FIRST, timestamp, 3),
                        new StoredMessageId(BOOK_ID, "test-session-alias-2", SECOND, timestamp, 4)
                )
        );

        assertThat(deserializeBatchLinkedMessageIds(decodeHex(SERIALIZED_EVENTS), BOOK_ID))
                .usingRecursiveComparison().isEqualTo(target);
    }

    @Test
    public void testSerializeLinkedMessageIds() throws IOException {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Set<StoredMessageId> source = new LinkedHashSet<>();
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1));
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-1", SECOND, timestamp, 2));
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-2", FIRST, timestamp, 3));
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-2", SECOND, timestamp, 4));

        byte[] bytes = serializeLinkedMessageIds(source);
        assertEquals(encodeHexString(bytes), SERIALIZED_MESSAGE_IDS);
    }

    @Test
    public void testDeserializeLinkedMessageIds() throws DecoderException, IOException {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Set<StoredMessageId> target = Set.of(
                new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1),
                new StoredMessageId(BOOK_ID, "test-session-alias-1", SECOND, timestamp, 2),
                new StoredMessageId(BOOK_ID, "test-session-alias-2", FIRST, timestamp, 3),
                new StoredMessageId(BOOK_ID, "test-session-alias-2", SECOND, timestamp, 4)
        );

        assertThat(deserializeLinkedMessageIds(decodeHex(SERIALIZED_MESSAGE_IDS), BOOK_ID))
                .usingRecursiveComparison().isEqualTo(target);
    }

    @Test
    public void testSerializeLinkedMessageId() throws IOException {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Set<StoredMessageId> source = Set.of(
                new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1)
        );

        byte[] bytes = serializeLinkedMessageIds(source);
        assertEquals(encodeHexString(bytes), SERIALIZED_MESSAGE_ID);
    }

    @Test
    public void testDeserializeLinkedMessageId() throws DecoderException, IOException {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Set<StoredMessageId> target = Set.of(
                new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1)
        );

        assertThat(deserializeLinkedMessageIds(decodeHex(SERIALIZED_MESSAGE_ID), BOOK_ID))
                .usingRecursiveComparison().isEqualTo(target);
    }
}