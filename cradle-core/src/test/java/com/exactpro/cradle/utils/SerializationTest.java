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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class SerializationTest {
	
	@Test
	public void serializeDeserialize() throws SerializationException {
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setStreamName("streamname12345");
		builder.setIndex(123456789010111213L);
		builder.setDirection(Direction.SECOND);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
		builder.putMetadata("key1", "value1");
		builder.putMetadata("key2", "value2");
		builder.putMetadata("key3", "value3");
		builder.setContent("MessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessage".getBytes(StandardCharsets.UTF_8));
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		byte[] serialize = serializer.serialize(build);
		MessageDeserializer deserializer = new MessageDeserializer();
		StoredMessage deserialize = deserializer.deserialize(serialize);
		Assert.assertEquals(deserialize, build);
	}

	@Test
	public void serializeDeserialize2() throws SerializationException {
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setStreamName("streamname12345");
		builder.setIndex(123456789010111213L);
		builder.setDirection(Direction.SECOND);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z").plusNanos(51234));
		builder.setContent("MessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessage".getBytes(StandardCharsets.UTF_8));
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		byte[] serialize = serializer.serialize(build);
		MessageDeserializer deserializer = new MessageDeserializer();
		StoredMessage deserialize = deserializer.deserialize(serialize);
		Assert.assertEquals(deserialize, build);
	}

	@Test
	public void serializeDeserialize3BIGStreamName() throws SerializationException {
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setStreamName("str3456789".repeat(65000/10));
		builder.setIndex(123456789010111213L);
		builder.setDirection(Direction.SECOND);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z").plusNanos(51234));
		builder.setContent("MessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessage".getBytes(StandardCharsets.UTF_8));
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		byte[] serialize = serializer.serialize(build);
		MessageDeserializer deserializer = new MessageDeserializer();
		StoredMessage deserialize = deserializer.deserialize(serialize);
		Assert.assertEquals(deserialize, build);
	}

	@Test
	public void serializeDeserialize4OverflowStreamName() throws SerializationException {
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setStreamName("str3456789".repeat(66000/10));
		builder.setIndex(123456789010111213L);
		builder.setDirection(Direction.SECOND);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z").plusNanos(51234));
		builder.setContent("MessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessage".getBytes(StandardCharsets.UTF_8));
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		try {
			serializer.serialize(build);
		} catch (SerializationException e) {
			Assert.assertTrue(true);
			return;
		}
		Assert.fail("Should be an error when stream name is more than 65536");
	}

	@Test
	public void checkMessageLength() throws SerializationException {
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setStreamName("streamname12345");
		builder.setIndex(123456789010111213L);
		builder.setDirection(Direction.SECOND);
		builder.setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z").plusNanos(51234));
		builder.setContent("MessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessageMessage".getBytes(StandardCharsets.UTF_8));
		StoredMessage build = builder.build();
		MessageSerializer serializer = new MessageSerializer();
		
		ByteBuffer buffer = ByteBuffer.allocate(10_000);
		serializer.serialize(build, buffer);
		Assert.assertEquals(buffer.position(), serializer.calculateMessageSize(build));
	}
	
}
