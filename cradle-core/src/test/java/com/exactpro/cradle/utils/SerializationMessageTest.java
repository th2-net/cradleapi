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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBuilder;
import com.exactpro.cradle.serialization.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SerializationMessageTest {

	@Test
	public void serializeDeserialize() throws SerializationException {
		MessageCommonParams commonParams = new MessageCommonParams();
		commonParams.setBookName("book_name1234");
		commonParams.setSessionAlias("stream_name12345");
		commonParams.setDirection(Direction.SECOND);

		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setSessionAlias(commonParams.getSessionAlias());
		builder.setIndex(123456789010111213L);
		builder.setDirection(commonParams.getDirection());
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
		builder.putMetadata("key1", "value1");
		builder.putMetadata("key2", "value2");
		builder.putMetadata("key3", "value3");
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));
		builder.setBookId(commonParams.getBookId());
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		byte[] serialize = serializer.serialize(build);
		MessageDeserializer deserializer = new MessageDeserializer();

		StoredMessage deserialize = deserializer.deserialize(serialize, commonParams);
		Assert.assertEquals(deserialize, build);
	}

	@Test
	public void serializeDeserialize2() throws SerializationException {
		MessageCommonParams commonParams = new MessageCommonParams();
		commonParams.setBookName("book_name1234");
		commonParams.setSessionAlias("stream_name12345");
		commonParams.setDirection(Direction.SECOND);

		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setSessionAlias(commonParams.getSessionAlias());
		builder.setIndex(123456789010111213L);
		builder.setDirection(Direction.SECOND);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z").plusNanos(51234));
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));
		builder.setBookId(commonParams.getBookId());
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		byte[] serialize = serializer.serialize(build);
		MessageDeserializer deserializer = new MessageDeserializer();
		StoredMessage deserialize = deserializer.deserialize(serialize, commonParams);
		Assert.assertEquals(deserialize, build);
	}

	@Test
	public void serializeDeserializeEmptyBody() throws SerializationException {
		MessageCommonParams commonParams = new MessageCommonParams();
		commonParams.setBookName("book_name1234");
		commonParams.setSessionAlias("stream_name12345");
		commonParams.setDirection(Direction.SECOND);

		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setSessionAlias(commonParams.getSessionAlias());
		builder.setIndex(123456789010111213L);
		builder.setDirection(commonParams.getDirection());
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
		builder.setContent(new byte[0]);
		builder.setBookId(commonParams.getBookId());
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		byte[] serialize = serializer.serialize(build);
		MessageDeserializer deserializer = new MessageDeserializer();

		StoredMessage deserialize = deserializer.deserialize(serialize, commonParams);
		Assert.assertEquals(deserialize, build);
	}

	@Test
	public void checkMessageLength() throws SerializationException {
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setSessionAlias("stream_name12345");
		builder.setIndex(123456789010111213L);
		builder.setDirection(Direction.SECOND);
		builder.setBookId(new BookId("book_name1234"));
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z").plusNanos(51234));
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();

		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serialize(build, buffer);
		Assert.assertEquals(buffer.position(), MessagesSizeCalculator.calculateMessageSize(build));
	}

	static MessageCommonParams getCommonParams() {
		MessageCommonParams commonParams = new MessageCommonParams();
		commonParams.setBookId(new BookId("book123456"));
		commonParams.setSessionAlias("stream_name12345");
		commonParams.setDirection(Direction.SECOND);
		return commonParams;
	}

	static List<StoredMessage> getBatch() {
		return getBatch(getCommonParams());
	}

	static List<StoredMessage> getBatch(MessageCommonParams params) {

		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setSessionAlias(params.getSessionAlias());
		builder.setIndex(123456789010111213L);
		builder.setDirection(params.getDirection());
		builder.setBookId(params.getBookId());
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
		builder.putMetadata("key1", "value1");
		builder.putMetadata("key2", "value2");
		builder.putMetadata("key3", "value3");
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));

		List<StoredMessage> stMessage = new ArrayList<>(10);
		stMessage.add(builder.build());

		builder.setIndex(123456789010111214L);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.01Z"));
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));
		stMessage.add(builder.build());

		builder.setIndex(123456789010111215L);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.01Z"));
		builder.setContent("Message".repeat(10).getBytes(StandardCharsets.UTF_8));
		stMessage.add(builder.build());

		builder.setIndex(123456789010111216L);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.02Z"));
		builder.setContent("Message2".repeat(10).getBytes(StandardCharsets.UTF_8));
		stMessage.add(builder.build());

		builder.setIndex(123456789010111217L);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.03Z"));
		builder.setContent("Message3".repeat(10).getBytes(StandardCharsets.UTF_8));
		stMessage.add(builder.build());

		return stMessage;
	}

	@Test
	public void serializeDeserializeBatch() throws SerializationException {
		MessageSerializer serializer = new MessageSerializer();
		MessageCommonParams commonParams = getCommonParams();
		List<StoredMessage> initBatch = getBatch(commonParams);
		byte[] serialize = serializer.serializeBatch(initBatch).getSerializedData();
		MessageDeserializer deserializer = new MessageDeserializer();
		List<StoredMessage> deserialize = deserializer.deserializeBatch(serialize, commonParams);
		Assert.assertEquals(deserialize, initBatch);
	}

	@Test
	public void serializeDeserializeEmptyBatch() throws SerializationException {
		MessageSerializer serializer = new MessageSerializer();
		MessageCommonParams commonParams = getCommonParams();
		List<StoredMessage> initBatch = Collections.emptyList();
		byte[] serialize = serializer.serializeBatch(initBatch).getSerializedData();
		MessageDeserializer deserializer = new MessageDeserializer();
		List<StoredMessage> deserialize = deserializer.deserializeBatch(serialize, commonParams);
		Assert.assertEquals(deserialize, initBatch);
	}

	@Test
	public void checkMessageBatchLength() throws SerializationException {
		MessageSerializer serializer = new MessageSerializer();

		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		List<StoredMessage> batch = getBatch();
		serializer.serializeBatch(batch, buffer, null);
		Assert.assertEquals(buffer.position(), MessagesSizeCalculator.calculateMessageBatchSize(batch).total);
	}

}
