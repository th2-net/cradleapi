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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.exactpro.cradle.Direction.FIRST;
import static com.exactpro.cradle.Direction.SECOND;
import static com.exactpro.cradle.serialization.EventMessageIdSerializer2.serializeBatchLinkedMessageIds;
import static com.exactpro.cradle.serialization.EventMessageIdSerializer2.serializeLinkedMessageIds;
import static org.assertj.core.util.Hexadecimals.toHexString;
import static org.testng.Assert.assertEquals;

public class EventMessageIdSerializer2Test {

    private static final BookId BOOK_ID = new BookId("test-book");
    private static final String SCOPE = "test-scope";

    @Test
    public void testSerializeBatchLinkedMessageIds() {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Map<StoredTestEventId, Set<StoredMessageId>> source = new LinkedHashMap<>();
        Set<StoredMessageId> ids = new LinkedHashSet<>();
        ids.add(new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1));
        ids.add(new StoredMessageId(BOOK_ID, "test-session-alias-1", SECOND, timestamp, 2));
        source.put(new StoredTestEventId(BOOK_ID, SCOPE, timestamp, "test-id-1"), ids);
        ids = new LinkedHashSet<>();
        ids.add(new StoredMessageId(BOOK_ID, "test-session-alias-2", FIRST, timestamp, 3));
        ids.add(new StoredMessageId(BOOK_ID, "test-session-alias-2", SECOND, timestamp, 4));
        source.put(new StoredTestEventId(BOOK_ID, SCOPE, timestamp, "test-id-2"), ids);

//        ByteBuffer buffer = serializeBatchLinkedMessageIds(source);
//        assertEquals(buffer.position(), buffer.limit());
//        assertEquals(buffer.capacity(), buffer.limit());
//        assertEquals(toHexString(buffer.array()),
//                "0201000200020014746573742D73657373696F6E2" +
//                        "D616C6961732D3100010014746573742D73657373" +
//                        "696F6E2D616C6961732D3200020000000065F0EC8" +
//                        "0000000000009746573742D69642D310002000101" +
//                        "0000000065F0EC800000000000000000000000010" +
//                        "001020000000065F0EC8000000000000000000000" +
//                        "00020000000065F0EC80000000000009746573742" +
//                        "D69642D3200020002010000000065F0EC80000000" +
//                        "0000000000000000030002020000000065F0EC800" +
//                        "00000000000000000000004");
    }

    @Test
    public void testSerializeLinkedMessageIds() {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Set<StoredMessageId> source = new LinkedHashSet<>();
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1));
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-1", SECOND, timestamp, 2));
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-2", FIRST, timestamp, 3));
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-2", SECOND, timestamp, 4));

        ByteBuffer buffer = serializeLinkedMessageIds(source);
        assertEquals(buffer.position(), buffer.limit());
        assertEquals(buffer.capacity(), buffer.limit());
        assertEquals(toHexString(buffer.array()),
                "0201000400020014746573742D73657373696F6" +
                        "E2D616C6961732D3100010014746573742D7365" +
                        "7373696F6E2D616C6961732D320002000101000" +
                        "0000065F0EC8000000000000000000000000100" +
                        "01020000000065F0EC800000000000000000000" +
                        "000020002010000000065F0EC80000000000000" +
                        "0000000000030002020000000065F0EC8000000" +
                        "0000000000000000004");
    }

    @Test
    public void testSerializeLinkedMessageId() {
        Instant timestamp = Instant.parse("2024-03-13T00:00:00Z");
        Set<StoredMessageId> source = new HashSet<>();
        source.add(new StoredMessageId(BOOK_ID, "test-session-alias-1", FIRST, timestamp, 1));

        ByteBuffer buffer = serializeLinkedMessageIds(source);
        assertEquals(buffer.position(), buffer.limit());
        assertEquals(buffer.capacity(), buffer.limit());
        assertEquals(toHexString(buffer.array()),
                "020100010014746573742D73657373696F6E2D61" +
                        "6C6961732D31010000000065F0EC8000000000000" +
                        "0000000000001");
    }
}
