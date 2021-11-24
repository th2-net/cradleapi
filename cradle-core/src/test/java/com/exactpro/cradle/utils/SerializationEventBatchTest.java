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

import com.exactpro.cradle.serialization.EventBatchDeserializer;
import com.exactpro.cradle.serialization.EventBatchSerializer;
import com.exactpro.cradle.serialization.SerializationException;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.BatchedStoredTestEventBuilder;
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata;
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadataBuilder;
import com.exactpro.cradle.testevents.StoredTestEventId;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializationEventBatchTest {

	@Test
	public void checkSize1() throws SerializationException {
		BatchedStoredTestEvent build = createBatchedStoredTestEvent("Test even1234567890");
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventRecord(build, buffer);
		Assert.assertEquals(buffer.position(), serializer.calculateEventRecordSize(build));
	}

	@Test
	public void checkSize2() throws SerializationException {
		BatchedStoredTestEventMetadata build = createBatchedStoredTestEventMetadata("Test even1234567890");
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventMetadataRecord(build, buffer);
		Assert.assertEquals(buffer.position(), serializer.calculateEventMetadataSize(build));
	}

	@Test
	public void checkSize3() throws SerializationException {
		Collection<BatchedStoredTestEvent> build = createBatchEvents();
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventBatch(build, buffer);
		Assert.assertEquals(buffer.position(), serializer.calculateBatchEventSize(build).total);
	}

	@Test
	public void checkSize4() throws SerializationException {
		Collection<BatchedStoredTestEventMetadata> build = createBatchMetadata();
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventMetadataBatch(build, buffer);
		Assert.assertEquals(buffer.position(), serializer.calculateBatchEventMetadataSize(build).total);
	}
	

	@Test
	public void serializeDeserialize() throws SerializationException {
		BatchedStoredTestEvent build = createBatchedStoredTestEvent("Test even1234567890");
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventRecord(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		BatchedStoredTestEvent deserialize = deserializer.deserializeBatchEntry(serialize);
		assertThat(build).usingRecursiveComparison().isEqualTo(deserialize);
	}

	
	@Test
	public void serializeDeserialize2() throws SerializationException {
		BatchedStoredTestEventMetadata build = createBatchedStoredTestEventMetadata("Test even1234567890");
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventMetadataRecord(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		BatchedStoredTestEventMetadata deserialize = deserializer.deserializeBatchEntryMetadata(serialize);
		assertThat(build).usingRecursiveComparison().isEqualTo(deserialize);
	}

	@Test
	public void serializeDeserialize3() throws Exception {
		List<BatchedStoredTestEvent> build = createBatchEvents();
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventBatch(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		List<BatchedStoredTestEvent> deserialize = deserializer.deserializeBatchEntries(serialize);
		assertThat(build).usingRecursiveComparison().isEqualTo(deserialize);
	}

	@Test
	public void serializeDeserialize4() throws Exception {
		List<BatchedStoredTestEventMetadata> build = createBatchMetadata();
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventMetadataBatch(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		List<BatchedStoredTestEventMetadata> deserialize = deserializer.deserializeBatchEntriesMetadata(serialize);
		assertThat(build).usingRecursiveComparison().isEqualTo(deserialize);
	}

	static BatchedStoredTestEventMetadata createBatchedStoredTestEventMetadata(String name) {
		BatchedStoredTestEventMetadataBuilder builder = new BatchedStoredTestEventMetadataBuilder();
		builder.setSuccess(true);
		builder.setStartTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
		builder.setEndTimestamp(Instant.parse("2007-12-03T10:15:31.00Z"));
		builder.setId(new StoredTestEventId(UUID.randomUUID().toString()));
		builder.setParentId(new StoredTestEventId(UUID.randomUUID().toString()));
		builder.setName(name);
		builder.setType(name + " ----");
		return builder.build();
	}

	static BatchedStoredTestEvent createBatchedStoredTestEvent(String name) {
		BatchedStoredTestEventBuilder builder = new BatchedStoredTestEventBuilder();
		builder.setSuccess(true);
		builder.setStartTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
		builder.setEndTimestamp(Instant.parse("2007-12-03T10:15:31.00Z"));
		builder.setId(new StoredTestEventId(UUID.randomUUID().toString()));
		builder.setParentId(new StoredTestEventId(UUID.randomUUID().toString()));
		builder.setName(name);
		builder.setType(name + " ----");
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));
		return builder.build();
	}

	static List<BatchedStoredTestEvent> createBatchEvents() {
		ArrayList<BatchedStoredTestEvent> objects = new ArrayList<>(3);
		objects.add(createBatchedStoredTestEvent("batch1"));
		objects.add(createBatchedStoredTestEvent("batch2"));
		objects.add(createBatchedStoredTestEvent("batch3"));
		return objects;
	}

	static List<BatchedStoredTestEventMetadata> createBatchMetadata() {
		ArrayList<BatchedStoredTestEventMetadata> objects = new ArrayList<>(3);
		objects.add(createBatchedStoredTestEventMetadata("batch1"));
		objects.add(createBatchedStoredTestEventMetadata("batch2"));
		objects.add(createBatchedStoredTestEventMetadata("batch3"));
		return objects;
	}
}
