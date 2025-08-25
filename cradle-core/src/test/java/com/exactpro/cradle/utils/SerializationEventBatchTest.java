/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.BatchedStoredTestEventBuilder;
import com.exactpro.cradle.testevents.StoredTestEventId;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.exactpro.cradle.TestUtils.generateUnicodeString;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateBatchEventSize;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateEventRecordSize;

public class SerializationEventBatchTest {

	@Test
	public void checkSize1() throws SerializationException {
		BatchedStoredTestEvent build = createBatchedStoredTestEvent("Test even1234567890", createCommonParams());
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventRecord(build, buffer);
		Assert.assertEquals(buffer.position(), calculateEventRecordSize(build));
	}

	@Test
	public void checkSize2() throws SerializationException {
		Collection<BatchedStoredTestEvent> build = createBatchEvents();
		EventBatchSerializer serializer = new EventBatchSerializer();
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serializeEventBatch(build, buffer);
		Assert.assertEquals(buffer.position(), calculateBatchEventSize(build).total);
	}


	@Test
	public void serializeDeserialize() throws SerializationException {
		EventBatchCommonParams commonParams = createCommonParams();
		BatchedStoredTestEvent build = createBatchedStoredTestEvent("Test even1234567890", commonParams);
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventRecord(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		BatchedStoredTestEvent deserialize = deserializer.deserializeBatchEntry(serialize, commonParams);

		RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();
		config.ignoreFields("bufferedContent");
		Assertions.assertThat(deserialize).usingRecursiveComparison(config).isEqualTo(build);
	}


	@Test
	public void serializeDeserialize2() throws Exception {
		EventBatchCommonParams commonParams = createCommonParams();
		List<BatchedStoredTestEvent> build = createBatchEvents(commonParams);
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventBatch(build).getSerializedData();
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		List<BatchedStoredTestEvent> deserialize = deserializer.deserializeBatchEntries(serialize, commonParams);
		RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();
		config.ignoreFields("bufferedContent");
		Assertions.assertThat(build).usingRecursiveFieldByFieldElementComparator(config).isEqualTo(deserialize);
	}

	@Test
	public void serializeDeserialize3UnicodeCharacters() throws Exception {
		EventBatchCommonParams commonParams = createCommonParams();
		String name = generateUnicodeString((1 << 18), 50);
		String content = generateUnicodeString((1 << 19), 10);
		BatchedStoredTestEvent build = createBatchedStoredTestEventWithContent(name, commonParams, content);
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialized = serializer.serializeEventRecord(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		BatchedStoredTestEvent deserialized = deserializer.deserializeBatchEntry(serialized, commonParams);
		RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();
		config.ignoreFields("bufferedContent");
		Assertions.assertThat(deserialized).usingRecursiveComparison(config).isEqualTo(build);
	}

	static BatchedStoredTestEvent createBatchedStoredTestEvent(String name, EventBatchCommonParams commonParams) {
		return createBatchedStoredTestEventWithContent(name, commonParams, "Message");
	}

	static BatchedStoredTestEvent createBatchedStoredTestEventWithContent(String name, EventBatchCommonParams commonParams, String content) {
		BatchedStoredTestEventBuilder builder = new BatchedStoredTestEventBuilder();
		builder.setSuccess(true);
		Instant startTime = Instant.parse("2007-12-03T10:15:30.00Z");
		String scope = commonParams.getScope();
		builder.setEndTimestamp(Instant.parse("2007-12-03T10:15:31.00Z"));
		builder.setId(new StoredTestEventId(commonParams.getBookId(), scope, startTime, UUID.randomUUID().toString()));
		builder.setParentId(new StoredTestEventId(commonParams.getBookId(), scope, startTime, UUID.randomUUID().toString()));
		builder.setName(name);
		builder.setType(name + " ----");
		builder.setContent(content.repeat(10).getBytes(StandardCharsets.UTF_8));
		return builder.build();
	}

	static List<BatchedStoredTestEvent> createBatchEvents() {
		return createBatchEvents(createCommonParams());
	}

	static List<BatchedStoredTestEvent> createBatchEvents(EventBatchCommonParams commonParams) {
		ArrayList<BatchedStoredTestEvent> objects = new ArrayList<>(3);
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
