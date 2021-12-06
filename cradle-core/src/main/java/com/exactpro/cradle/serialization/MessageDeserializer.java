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

package com.exactpro.cradle.serialization;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBuilder;
import com.exactpro.cradle.messages.StoredMessageId;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.INVALID_MAGIC_NUMBER_FORMAT;
import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.*;
import static com.exactpro.cradle.serialization.Serialization.NOT_SUPPORTED_PROTOCOL_FORMAT;
import static com.exactpro.cradle.serialization.SerializationUtils.readInstant;
import static com.exactpro.cradle.serialization.SerializationUtils.readShortString;
import static com.exactpro.cradle.serialization.SerializationUtils.readString;
import static com.exactpro.cradle.serialization.SerializationUtils.readBody;

public class MessageDeserializer {

	public boolean checkMessageBatchHeader(byte[] array) {
		return ByteBuffer.wrap(array, 0, 4).getInt() == MESSAGE_BATCH_MAGIC;
	}
	
	public List<StoredMessage> deserializeBatch(byte[] buffer) throws SerializationException {
		return this.deserializeBatch(ByteBuffer.wrap(buffer));
	}
	
	private void checkMessageBatchMagics(ByteBuffer buffer) throws SerializationException {
		int magicNumber = buffer.getInt();
		if (magicNumber != MESSAGE_BATCH_MAGIC) {
			throw SerializationUtils.incorrectMagicNumber("MESSAGE_BATCH", magicNumber, MESSAGE_BATCH_MAGIC);
		}

		byte protocolVer = buffer.get();
		if (protocolVer != MESSAGE_PROTOCOL_VER) {
			throw new SerializationException(String.format(NOT_SUPPORTED_PROTOCOL_FORMAT, "message batches",
					protocolVer, MESSAGE_PROTOCOL_VER));
		}
	}
	
	public List<StoredMessage> deserializeBatch(ByteBuffer buffer) throws SerializationException {
		checkMessageBatchMagics(buffer);

		String sessionAlias = SerializationUtils.readShortString(buffer);
		Direction direction = getDirection(buffer.get());

		int messagesCount = buffer.getInt();
		List<StoredMessage> messages = new ArrayList<>(messagesCount);
		for (int i = 0; i < messagesCount; ++i) {
			int msgLen = buffer.getInt();
			ByteBuffer msgBuf = ByteBuffer.wrap(buffer.array(), buffer.position(), msgLen);
			messages.add(this.deserialize(msgBuf, sessionAlias, direction));
			buffer.position(buffer.position() + msgLen);
		}
		
		return messages;
	}

	public StoredMessage deserializeOneMessage(ByteBuffer buffer, StoredMessageId id) throws SerializationException {
		checkMessageBatchMagics(buffer);

		String sessionAlias = SerializationUtils.readShortString(buffer);
		Direction direction = getDirection(buffer.get());
		
		int messagesCount = buffer.getInt();
		for (int i = 0; i < messagesCount; ++i) {
			int msgLen = buffer.getInt();
			ByteBuffer msgBuf = ByteBuffer.wrap(buffer.array(), buffer.position(), msgLen);
			StoredMessage msg = this.deserialize(msgBuf, sessionAlias, direction);
			if (msg.getId().equals(id)) {
				return msg;
			}
			buffer.position(buffer.position() + msgLen);
		}

		return null;
	}
	
	public StoredMessage deserialize(byte[] message, String sessionAlias, Direction dir) throws SerializationException {
		return deserialize(ByteBuffer.wrap(message), sessionAlias, dir);
	}

	public StoredMessage deserialize(ByteBuffer buffer, String sessionAlias, Direction dir) throws SerializationException {
		short magicNumber = buffer.getShort();
		if (magicNumber != MESSAGE_MAGIC) {
			throw SerializationUtils.incorrectMagicNumber(StoredMessage.class.getSimpleName(), magicNumber, MESSAGE_MAGIC);
		}
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setMessageId(readMessageId(buffer, sessionAlias, dir));
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

	private StoredMessageId readMessageId(ByteBuffer buffer, String stream, Direction direction) throws SerializationException {
		long index = buffer.getLong();
		return new StoredMessageId(stream, direction, index);
	}

	private void readMessageMetaData(ByteBuffer buffer, StoredMessageBuilder builder) throws SerializationException {
		int len = buffer.getInt();
		for (int i = 0; i < len; ++i) {
			String key = readString(buffer);
			String value = readString(buffer);
			builder.putMetadata(key, value);
		}
	}
	
}
