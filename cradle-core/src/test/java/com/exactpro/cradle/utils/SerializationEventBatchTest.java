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
import com.exactpro.cradle.testevents.BatchedStoredTestEventBuilder;
import com.exactpro.cradle.testevents.StoredTestEventId;
import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

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
	public void checkSize3() throws SerializationException {
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
		Assertions.assertThat(deserialize).usingRecursiveComparison().isEqualTo(build);
	}


	@Test
	public void serializeDeserialize3() throws Exception {
		EventBatchCommonParams commonParams = createCommonParams();
		List<BatchedStoredTestEvent> build = createBatchEvents(commonParams);
		EventBatchSerializer serializer = new EventBatchSerializer();
		byte[] serialize = serializer.serializeEventBatch(build);
		EventBatchDeserializer deserializer = new EventBatchDeserializer();
		List<BatchedStoredTestEvent> deserialize = deserializer.deserializeBatchEntries(serialize, commonParams);
		Assertions.assertThat(build).usingRecursiveFieldByFieldElementComparator().isEqualTo(deserialize);
	}

	static BatchedStoredTestEvent createBatchedStoredTestEvent(String name, EventBatchCommonParams commonParams) {
		BatchedStoredTestEventBuilder builder = new BatchedStoredTestEventBuilder();
		builder.setSuccess(true);
		Instant startTime = Instant.parse("2007-12-03T10:15:30.00Z");
		String scope = commonParams.getScope();
		builder.setEndTimestamp(Instant.parse("2007-12-03T10:15:31.00Z"));
		builder.setId(new StoredTestEventId(commonParams.getBookId(), scope, startTime, UUID.randomUUID().toString()));
		builder.setParentId(new StoredTestEventId(commonParams.getBookId(), scope, startTime, UUID.randomUUID().toString()));
		builder.setName(name);
		builder.setType(name + " ----");
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));
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
