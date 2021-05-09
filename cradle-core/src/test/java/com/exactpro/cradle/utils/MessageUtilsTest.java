/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.utils;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

public class MessageUtilsTest
{
	private StoredMessageBatch batch;
	private StoredMessage msg1,
			msg2;
	
	@BeforeClass
	public void prepare() throws CradleStorageException
	{
		MessageToStoreBuilder builder = new MessageToStoreBuilder();
		String streamName = "Stream1";
		Direction direction = Direction.FIRST;
		Instant timestamp = Instant.now();
		byte[] content = "Message text".getBytes();
		
		long index = 10;
		batch = new StoredMessageBatch();
		msg1 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.index(10)
				.timestamp(timestamp)
				.content(content)
				.build());
		
		msg2 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.index(index+10)  //Need to have a gap between indices to verify that messages are written/read correctly
				.timestamp(timestamp)
				.content(content)
				.build());
	}
	
	@Test
	public void messageIds() throws IOException
	{
		byte[] bytes = MessageUtils.serializeMessages(batch.getMessages());
		
		Collection<StoredMessage> restored = MessageUtils.deserializeMessages(bytes);
		Iterator<StoredMessage> it = restored.iterator();
		
		Assert.assertEquals(it.next().getId(), msg1.getId(), "1st message ID");
		Assert.assertEquals(it.next().getId(), msg2.getId(), "2nd message ID");
	}
	
	@Test
	public void oneMessageId() throws IOException
	{
		byte[] bytes = MessageUtils.serializeMessages(batch.getMessages());
		StoredMessage restored = MessageUtils.deserializeOneMessage(bytes, msg2.getId());
		Assert.assertEquals(restored.getId(), msg2.getId(), "ID of requested message");
	}
}
