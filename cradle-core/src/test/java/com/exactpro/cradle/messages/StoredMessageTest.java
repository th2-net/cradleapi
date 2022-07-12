/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.messages;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class StoredMessageTest {

    @Test
    public void messageSizeTest() {

        MessageToStore toStore = new MessageToStore();
        toStore.setIndex(12345L);
        toStore.setDirection(Direction.FIRST);
        toStore.setStreamName("test");
        toStore.setTimestamp(Instant.EPOCH);
        toStore.setContent("Message1234567890".repeat(10).getBytes(StandardCharsets.UTF_8));

        StoredMessageId id = new StoredMessageId(toStore.getStreamName(), toStore.getDirection(), toStore.getIndex());
        StoredMessage msg = new StoredMessage(toStore, id);

        Assert.assertEquals(MessagesSizeCalculator.calculateMessageSize(toStore),
                MessagesSizeCalculator.calculateMessageSize(msg));
        Assert.assertEquals(MessagesSizeCalculator.calculateMessageSizeInBatch(toStore),
                MessagesSizeCalculator.calculateMessageSizeInBatch(msg));
    }

    @Test
    public void protocolTest() {

        final String protocol = "test_protocol";
        StoredMessageId id = new StoredMessageId("test", Direction.FIRST, 12345);
        MessageMetadata metadata = new MessageMetadata();
        metadata.add(StoredMessage.METADATA_KEY_PROTOCOL, protocol);

        StoredMessage msg = new StoredMessage(id, Instant.EPOCH, metadata,
                "Message1234567890".repeat(10).getBytes(StandardCharsets.UTF_8));


        Assert.assertEquals(protocol, msg.getProtocol());
    }


    @Test
    public void emptyProtocolTest() {

        StoredMessageId id = new StoredMessageId("test", Direction.FIRST, 12345);

        StoredMessage msg = new StoredMessage(id, Instant.EPOCH, null,
                "Message1234567890".repeat(10).getBytes(StandardCharsets.UTF_8));


        Assert.assertNull(msg.getProtocol());
    }

}
