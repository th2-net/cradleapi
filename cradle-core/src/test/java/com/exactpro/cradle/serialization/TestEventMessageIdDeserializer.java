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

import org.testng.annotations.Test;

import java.io.IOException;

public class TestEventMessageIdDeserializer {

    @Test(
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "unknown id mark 3"
    )
    public void testDeserializeBatchLinkedMessageIds() throws IOException {
        byte[] data = {
                0x01,
                0x02,
                0x00, 0x00, 0x00, 0x01, // link count
                0x00, 0x01, // mapping
                0x00, 0x01, // stream name length
                (byte) 'A',
                0x00, 0x00, // mapping index
                0x00, 0x01, // event ID length
                (byte) 'E',
                0x00, 0x00, 0x00, 0x01, // ids count
                0x00, 0x00, // mapping index
                0x02, // direction
                0x00, 0x00, 0x00, 0x01, // ids for dir
                0x03, // UNKNOWN ID TYPE,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // index,
                0x00, // end of data
        };
        EventMessageIdDeserializer.deserializeBatchLinkedMessageIds(data);
    }
}