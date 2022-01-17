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

package com.exactpro.cradle.serialization;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBuilder;
import com.exactpro.cradle.messages.StoredMessageId;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.*;
import static com.exactpro.cradle.serialization.Serialization.NOT_SUPPORTED_PROTOCOL_FORMAT;
import static com.exactpro.cradle.serialization.SerializationUtils.readInstant;
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

	private MessageCommonParams readCommonsParams(ByteBuffer buffer) throws SerializationException {
		MessageCommonParams commonParams = new MessageCommonParams();
		commonParams.setBookName(SerializationUtils.readString(buffer));
		commonParams.setSessionAlias(SerializationUtils.readShortString(buffer));
		commonParams.setDirection(getDirection(buffer.get()));
		return commonParams;
	}

	private StoredMessage readMessage(ByteBuffer buffer, MessageCommonParams commonParams) throws SerializationException {
		int msgLen = buffer.getInt();
		ByteBuffer msgBuf = ByteBuffer.wrap(buffer.array(), buffer.position(), msgLen);
		StoredMessage msg = this.deserialize(msgBuf, commonParams);
		buffer.position(buffer.position() + msgLen);
		return msg;
	}
	
	public List<StoredMessage> deserializeBatch(ByteBuffer buffer) throws SerializationException {
		checkMessageBatchMagics(buffer);
		MessageCommonParams commonParams = readCommonsParams(buffer);

		int messagesCount = buffer.getInt();
		List<StoredMessage> messages = new ArrayList<>(messagesCount);
		for (int i = 0; i < messagesCount; ++i) {
			messages.add(this.readMessage(buffer, commonParams));
		}
		
		return messages;
	}

	public StoredMessage deserializeOneMessage(ByteBuffer buffer, StoredMessageId id) throws SerializationException {
		checkMessageBatchMagics(buffer);
		MessageCommonParams commonParams = readCommonsParams(buffer);
		
		int messagesCount = buffer.getInt();
		for (int i = 0; i < messagesCount; ++i) {
			StoredMessage msg = this.readMessage(buffer, commonParams);
			if (msg.getId().equals(id)) {
				return msg;
			}
		}

		return null;
	}
	
	public StoredMessage deserialize(byte[] message, MessageCommonParams commonParams) throws SerializationException {
		return deserialize(ByteBuffer.wrap(message), commonParams);
	}

	public StoredMessage deserialize(ByteBuffer buffer, MessageCommonParams commonParams) throws SerializationException {
		short magicNumber = buffer.getShort();
		if (magicNumber != MESSAGE_MAGIC) {
			throw SerializationUtils.incorrectMagicNumber(StoredMessage.class.getSimpleName(), magicNumber, MESSAGE_MAGIC);
		}
		StoredMessageBuilder builder = new StoredMessageBuilder();
		builder.setMessageId(readMessageId(buffer, commonParams));

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

	private StoredMessageId readMessageId(ByteBuffer buffer, MessageCommonParams commonParams) throws SerializationException {
		long index = buffer.getLong();
		Instant time = readInstant(buffer);
		return new StoredMessageId(commonParams.getBookId(), commonParams.getSessionAlias(),
				commonParams.getDirection(), time, index);
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
