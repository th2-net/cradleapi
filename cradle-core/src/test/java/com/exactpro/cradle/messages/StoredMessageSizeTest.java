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

package com.exactpro.cradle.messages;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.serialization.SerializationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class StoredMessageSizeTest {

	@Test
	public void msgToStoreVsStoredMessage() throws SerializationException {

		MessageToStore toStore = new MessageToStore();
		toStore.setIndex(12345L);
		toStore.setDirection(Direction.FIRST);
		toStore.setStreamName("test");
		toStore.setTimestamp(Instant.EPOCH);
		toStore.setContent("Message1234567890".repeat(10).getBytes(StandardCharsets.UTF_8));

		StoredMessage msg = new StoredMessage(toStore, new StoredMessageId(toStore.getStreamName(), toStore.getDirection(), toStore.getIndex()));
		
		Assert.assertEquals(MessagesSizeCalculator.calculateMessageSize(toStore),
				MessagesSizeCalculator.calculateMessageSize(msg));
		Assert.assertEquals(MessagesSizeCalculator.calculateMessageSizeInBatch(toStore),
				MessagesSizeCalculator.calculateMessageSizeInBatch(msg));
	}
	
	
}
