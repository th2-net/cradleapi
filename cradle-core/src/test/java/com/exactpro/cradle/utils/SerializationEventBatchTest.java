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

package com.exactpro.cradle.utils;

import com.exactpro.cradle.serialization.EventBatchCommonParams;
import com.exactpro.cradle.serialization.EventBatchDeserializer;
import com.exactpro.cradle.serialization.EventBatchSerializer;
import com.exactpro.cradle.serialization.SerializationException;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.exactpro.cradle.CoreStorageSettings.DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;
import static com.exactpro.cradle.TestUtils.generateUnicodeString;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateBatchEventSize;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateEventRecordSize;

public class SerializationEventBatchTest {

	@Test
	public void checkSize1() throws CradleStorageException {
		TestEventSingleToStore build = createBatchedStoredTestEvent("Test even1234567890", createCommonParams());
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventRecord(build, buffer);
		Assert.assertEquals(buffer.position(), calculateEventRecordSize(build));
	}

	@Test
	public void checkSize2() throws CradleStorageException {
		Collection<TestEventSingleToStore> build = createBatchEvents();
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventBatch(build, buffer);
		Assert.assertEquals(buffer.position(), calculateBatchEventSize(build).total);
	}


	@Test
	public void serializeDeserialize() throws SerializationException, CradleStorageException {
		EventBatchCommonParams commonParams = createCommonParams();
		TestEventSingleToStore build = createBatchedStoredTestEvent("Test even1234567890", commonParams);
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventRecord(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		BatchedStoredTestEvent deserialize = deserializer.deserializeBatchEntry(serialize, commonParams);
		Assertions.assertThat(deserialize).usingRecursiveComparison().isEqualTo(build);
	}


	@Test
	public void serializeDeserialize2() throws Exception {
		EventBatchCommonParams commonParams = createCommonParams();
		List<TestEventSingleToStore> build = createBatchEvents(commonParams);
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventBatch(build).getSerializedData();
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		List<BatchedStoredTestEvent> deserialize = deserializer.deserializeBatchEntries(serialize, commonParams);
		Assertions.assertThat(build).usingRecursiveFieldByFieldElementComparator().isEqualTo(deserialize);
	}

	@Test
	public void serializeDeserialize3UnicodeCharacters() throws Exception {
		EventBatchCommonParams commonParams = createCommonParams();
		String name = generateUnicodeString((1 << 18), 50);
		String content = generateUnicodeString((1 << 19), 10);
		TestEventSingleToStore build = createBatchedStoredTestEventWithContent(name, commonParams, content);
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialized = serializer.serializeEventRecord(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		BatchedStoredTestEvent deserialized = deserializer.deserializeBatchEntry(serialized, commonParams);
		Assertions.assertThat(deserialized).usingRecursiveComparison().isEqualTo(build);
	}

	static TestEventSingleToStore createBatchedStoredTestEvent(String name, EventBatchCommonParams commonParams) throws CradleStorageException {
		return createBatchedStoredTestEventWithContent(name, commonParams, "Message");
	}

	static TestEventSingleToStore createBatchedStoredTestEventWithContent(String name, EventBatchCommonParams commonParams, String content) throws CradleStorageException {
		TestEventSingleToStoreBuilder builder = TestEventSingleToStore.builder(DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS);
		builder.success(true);
		Instant startTime = Instant.parse("2007-12-03T10:15:30.00Z");
		String scope = commonParams.getScope();
		builder.endTimestamp(Instant.parse("2007-12-03T10:15:31.00Z"));
		builder.id(new StoredTestEventId(commonParams.getBookId(), scope, startTime, UUID.randomUUID().toString()));
		builder.parentId(new StoredTestEventId(commonParams.getBookId(), scope, startTime, UUID.randomUUID().toString()));
		builder.name(name);
		builder.type(name + " ----");
		builder.content(content.repeat(10).getBytes(StandardCharsets.UTF_8));
		return builder.build();
	}

	static List<TestEventSingleToStore> createBatchEvents() throws CradleStorageException {
		return createBatchEvents(createCommonParams());
	}

	static List<TestEventSingleToStore> createBatchEvents(EventBatchCommonParams commonParams) throws CradleStorageException {
		ArrayList<TestEventSingleToStore> objects = new ArrayList<>(3);
		objects.add(createBatchedStoredTestEvent("batch1", commonParams));
		objects.add(createBatchedStoredTestEvent("batch2", commonParams));
		objects.add(createBatchedStoredTestEvent("batch3", commonParams));
		return objects;
	}

	static EventBatchCommonParams createCommonParams() {
		EventBatchCommonParams params = new EventBatchCommonParams();
		params.setBookName("book1234");
		params.setScope("scope123456");
		return params;
	}
}
