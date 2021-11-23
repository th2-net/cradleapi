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

package com.exactpro.cradle.utils;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.serialization.EventBatchSerializer;
import com.exactpro.cradle.serialization.MessageSerializer;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata;
import org.apache.commons.lang3.SerializationUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SerializationBackwardCompatibility {

	private static byte[] serializeMessages(Collection<StoredMessage> messages) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
			 DataOutputStream dos = new DataOutputStream(out))
		{
			for (StoredMessage msg : messages)
			{
				if (msg == null)  //For case of not full batch
					break;

				byte[] serializedMsg = SerializationUtils.serialize(msg);
				dos.writeInt(serializedMsg.length);
				dos.write(serializedMsg);
			}
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}

	private static byte[] serializeTestEvents(Collection<BatchedStoredTestEvent> testEvents) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
			 DataOutputStream dos = new DataOutputStream(out))
		{
			for (BatchedStoredTestEvent te : testEvents)
				serialize(te, dos);
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}

	private static void serialize(Serializable data, DataOutputStream target) throws IOException
	{
		byte[] serializedData = SerializationUtils.serialize(data);
		target.writeInt(serializedData.length);
		target.write(serializedData);
	}

	private static byte[] serializeTestEventsMetadata(Collection<BatchedStoredTestEventMetadata> testEventsMetadata) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
			 DataOutputStream dos = new DataOutputStream(out))
		{
			for (BatchedStoredTestEventMetadata te : testEventsMetadata)
				serialize(te, dos);
			dos.flush();
			batchContent = CompressionUtils.compressData(out.toByteArray());
		}
		return batchContent;
	}
	
	@Test
	public void oldMsgSerializationTest() throws IOException {
		List<StoredMessage> batch = SerializationMessageTest.getBatch();
		byte[] bytes = serializeMessages(batch);
		List<StoredMessage> batch2 = MessageUtils.deserializeMessages(bytes);
		Assert.assertEquals(batch, batch2);
	}

	@Test
	public void newMsgSerializationTest() throws IOException {
		List<StoredMessage> batch = SerializationMessageTest.getBatch();
		byte[] bytes = new MessageSerializer().serializeBatch(batch);
		List<StoredMessage> batch2 = MessageUtils.deserializeMessages(bytes);
		Assert.assertEquals(batch, batch2);
	}

	@Test
	public void oldEventSerializationTest() throws Exception {
		List<BatchedStoredTestEvent> batch = SerializationEventBatchTest.createBatchEvents();
		byte[] bytes = serializeTestEvents(batch);
		ArrayList<BatchedStoredTestEvent> events = new ArrayList<>();
		TestEventUtils.deserializeTestEvents(bytes, events::add);
		for (int i = 0, to = Math.max(batch.size(), events.size()); i < to; ++i) {
			SerializationEventBatchTest.compare(batch.get(i), events.get(i));
		}
	}

	@Test
	public void newEventSerializationTest() throws Exception {
		List<BatchedStoredTestEvent> batch = SerializationEventBatchTest.createBatchEvents();
		byte[] bytes = new EventBatchSerializer().serializeEventBatch(batch);
		ArrayList<BatchedStoredTestEvent> events = new ArrayList<>();
		TestEventUtils.deserializeTestEvents(bytes, events::add);
		for (int i = 0, to = Math.max(batch.size(), events.size()); i < to; ++i) {
			SerializationEventBatchTest.compare(batch.get(i), events.get(i));
		}
	}

	@Test
	public void oldEventMDSerializationTest() throws Exception {
		List<BatchedStoredTestEventMetadata> batch = SerializationEventBatchTest.createBatchMetadata();
		byte[] bytes = serializeTestEventsMetadata(batch);
		ArrayList<BatchedStoredTestEventMetadata> events = new ArrayList<>();
		TestEventUtils.deserializeTestEventsMetadata(bytes, "batch123", events::add);
		for (int i = 0, to = Math.max(batch.size(), events.size()); i < to; ++i) {
			SerializationEventBatchTest.compare(batch.get(i), events.get(i));
		}
	}
	

	@Test
	public void newEventMDSerializationTest() throws Exception {
		List<BatchedStoredTestEventMetadata> batch = SerializationEventBatchTest.createBatchMetadata();
		byte[] bytes = new EventBatchSerializer().serializeEventMetadataBatch(batch);
		ArrayList<BatchedStoredTestEventMetadata> events = new ArrayList<>();
		TestEventUtils.deserializeTestEventsMetadata(bytes, "batch123", events::add);
		for (int i = 0, to = Math.max(batch.size(), events.size()); i < to; ++i) {
			SerializationEventBatchTest.compare(batch.get(i), events.get(i));
		}
	}

}
