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
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.messages.StoredMessageMetadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

public class MessageSerializer {
	
	public byte[] serialize(StoredMessage message) throws SerializationException {
		ByteBuffer b = ByteBuffer.allocate(calculateMessageSize(message));
		this.serialize(message, b);
		return b.array();
	}

	public void serialize(StoredMessage message, ByteBuffer buffer) throws SerializationException {
		buffer.putLong(StoredMessage.serialVersionUID);
		this.printMessageId(message.getId(), buffer);
		this.printInstant(message.getTimestamp(), buffer);
		this.printMessageMetaData(message.getMetadata(), buffer);
		this.printBody(message.getContent(), buffer);
	}
	
	public int calculateMessageSize(StoredMessage message) {
		
		/* 
		 8 - magic number
		 2 - stream id length
		 1 - DIRECTION enum (ordinal)
		 8 - index (long)
		 4 + 8 = Instant (timestamp) long (seconds) + int (nanos)
		 4 - message body (byte[]) length
		 4 - metadata (map) length
		 
		 Collapsed constant = 39 
		 */

		int i = message.getId().getStreamName().length() 
			+ message.getContent().length + 39;
		Map<String, String> md ;
		if (message.getMetadata() != null && (md = message.getMetadata().toMap()) != null) {
			for (Map.Entry<String, String> entry : md.entrySet()) {
				i += entry.getKey().length()  // key
					+ entry.getValue().length() + 8; // value + 2 length
			}
		}
		return i;
	}
	
	private void printMessageId(StoredMessageId messageId, ByteBuffer buffer) throws SerializationException {
		printStreamName(messageId.getStreamName(), buffer);
		buffer.put((byte) messageId.getDirection().ordinal());
		buffer.putLong(messageId.getIndex());
	}

	private void printInstant(Instant instant, ByteBuffer buffer) {
		buffer.putLong(instant.getEpochSecond());
		buffer.putInt(instant.getNano());
	}

	private void printMessageMetaData(StoredMessageMetadata metadata, ByteBuffer buffer) {
		if (metadata == null) {
			buffer.putInt(0);
		} else {
			Map<String, String> data = metadata.toMap();
			buffer.putInt(data.size());
			for (Map.Entry<String, String> entry : data.entrySet()) {
				printString(entry.getKey(), buffer);
				printString(entry.getValue(), buffer);
			}
		}
	}

	private void printBody(byte[] body, ByteBuffer buffer) {
		buffer.putInt(body.length);
		buffer.put(body);
	}

	private void printStreamName(String value, ByteBuffer buffer) throws SerializationException {
		if (value == null) {
			value = "";
		}
		if (value.length() > 65535) {
			throw new SerializationException("Session alias is too big. Expected [0-65535]");
		}
		buffer.putShort((short) value.length());
		buffer.put(value.getBytes(StandardCharsets.UTF_8));
	}
	
	private void printString(String value, ByteBuffer buffer) {
		if (value == null) {
			value = "";
		}
		buffer.putInt(value.length());
		buffer.put(value.getBytes(StandardCharsets.UTF_8));
	}
	
}
