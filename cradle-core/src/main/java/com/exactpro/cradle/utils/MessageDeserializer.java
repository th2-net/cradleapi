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
import com.exactpro.cradle.messages.StoredMessageId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class MessageDeserializer {
	
	public static final String INVALID_MAGIC_NUMBER_FORMAT = "Invalid magic number for class: %s. Got: %d. Expected %d. Probably received inconsistent data.";
	
	public StoredMessage deserialize(byte[] message) throws SerializationException {
		ByteBuffer buffer = ByteBuffer.wrap(message);
		long magicNumber = buffer.getLong();
		if (magicNumber != StoredMessage.serialVersionUID) {
			throw new SerializationException(String.format(INVALID_MAGIC_NUMBER_FORMAT,
					StoredMessage.class.getSimpleName(), magicNumber, StoredMessage.serialVersionUID));
		}
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setMessageId(readMessageId(buffer));
		builder.setTimestamp(readInstant(buffer));
		readMessageMetaData(buffer, builder);
		builder.setContent(readBody(buffer));
		return builder.build();
	}
	
	private Direction getDirection(int ordinal) throws SerializationException {
		Direction[] values = Direction.values();
		if (values.length > ordinal && ordinal >= 0)
			return values[ordinal];
		throw new SerializationException(String.format("Invalid ordinal for enum (Direction): %d. Values: [0-%d]",
				ordinal, values.length - 1));
	}

	private StoredMessageId readMessageId(ByteBuffer buffer) throws SerializationException {
		String stream = readStreamName(buffer);
		Direction direction = getDirection(buffer.get());
		long index = buffer.getLong();
		return new StoredMessageId(stream, direction, index);
	}

	private Instant readInstant(ByteBuffer buffer) {
		return Instant.ofEpochSecond(buffer.getLong(), buffer.getInt());
	}

	private void readMessageMetaData(ByteBuffer buffer, StoredMessageBuilder builder) throws SerializationException {
		int len = buffer.getInt();
		for (int i = 0; i < len; ++i) {
			String key = readString(buffer);
			String value = readString(buffer);
			builder.putMetadata(key, value);
		}
	}

	private byte[] readBody(ByteBuffer buffer) {
		int bodyLen = buffer.getInt();
		byte[] body = new byte[bodyLen];
		buffer.get(body);
		return body;
	}

	private String readString(ByteBuffer buffer) throws SerializationException {
		return readString(buffer, buffer.getInt());
	}

	private String readStreamName(ByteBuffer buffer) throws SerializationException {
		return readString(buffer, Short.toUnsignedInt(buffer.getShort()));
	}

	private String readString(ByteBuffer buffer, int len) throws SerializationException {
		if (len <= 0)
			return "";

		if (buffer.remaining() < len) {
			throw new SerializationException(String.format("Expected string (%d bytes) is bigger than remaining buffer (%d)",
					len, buffer.remaining()));
		}
		int currPos = buffer.position();
		String str = new String(buffer.array(), currPos, len, StandardCharsets.UTF_8);
		buffer.position(currPos + len);
		return str;
	}
	
}
