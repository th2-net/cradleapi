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

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.exactpro.cradle.Direction.FIRST;
import static com.exactpro.cradle.Direction.SECOND;
import static com.exactpro.cradle.serialization.EventMessageIdSerializer.serializeBatchLinkedMessageIds;
import static org.assertj.core.util.Hexadecimals.toHexString;
import static org.testng.Assert.assertEquals;

public class EventMessageIdSerializerTest {

    private static final BookId BOOK_ID = new BookId("test-book");
    private static final String SCOPE = "test-scope";

    @Test
    public void testSerializeBatchLinkedMessageIds() throws IOException {
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

        assertEquals(toHexString(serializeBatchLinkedMessageIds(source)),
                "01020000000200020014746573742D7365737369" +
                        "6F6E2D616C6961732D3100000014746573742D736" +
                        "57373696F6E2D616C6961732D320001000A746573" +
                        "742D73636F70650000000065F0EC8000000000000" +
                        "9746573742D69642D310000000200000100000001" +
                        "0000000065F0EC800000000000000000000000010" +
                        "2000000010000000065F0EC800000000000000000" +
                        "0000000200000A746573742D73636F70650000000" +
                        "065F0EC80000000000009746573742D69642D3200" +
                        "000002000101000000010000000065F0EC8000000" +
                        "000000000000000000302000000010000000065F0" +
                        "EC8000000000000000000000000400");
    }
}
