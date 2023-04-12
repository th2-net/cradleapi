/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class TestEventMessageIdSerializer {

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "prohibited sequence -9223372036854775808 for direction FIRST"
    )
    public void testSerializeBatchLinkedMessageIds() throws IOException, NoSuchFieldException, IllegalAccessException {
        var messageId = new StoredMessageId("test", Direction.FIRST, 0);
        setNegativeIndex(messageId, Long.MIN_VALUE);

        EventMessageIdSerializer.serializeBatchLinkedMessageIds(Map.of(
                new StoredTestEventId("test"), List.of(messageId)
        ));
    }

    private static void setNegativeIndex(StoredMessageId messageId, long index) throws NoSuchFieldException, IllegalAccessException {
        // prepare illegal state for message ID
        Field indexField = messageId.getClass().getDeclaredField("index");
        indexField.setAccessible(true);
        indexField.set(messageId, index);
    }
}