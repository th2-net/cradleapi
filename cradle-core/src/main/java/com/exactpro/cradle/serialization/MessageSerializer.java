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

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.messages.StoredMessageMetadata;
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.*;
import static com.exactpro.cradle.serialization.SerializationUtils.printBody;
import static com.exactpro.cradle.serialization.SerializationUtils.printInstant;
import static com.exactpro.cradle.serialization.SerializationUtils.printShortString;
import static com.exactpro.cradle.serialization.SerializationUtils.printString;

public class MessageSerializer {

	public byte[] serializeBatch(Collection<StoredMessage> batch) throws SerializationException {
		SerializationBatchSizes messageBatchSizes = calculateMessageBatchSize(batch);
		ByteBuffer buffer = ByteBuffer.allocate(messageBatchSizes.total);
		
		this.serializeBatch(batch, buffer, messageBatchSizes);
		
		return buffer.array();
	}

	public void serializeBatch(Collection<StoredMessage> batch, ByteBuffer buffer, SerializationBatchSizes messageBatchSizes) throws SerializationException {

		if (messageBatchSizes == null) {
			messageBatchSizes = this.calculateMessageBatchSize(batch);
		}
		
		buffer.putInt(MESSAGE_BATCH_MAGIC);
		buffer.put(MESSAGE_PROTOCOL_VER);
		buffer.putInt(batch.size());
		int i = 0;
		for (StoredMessage message : batch) {
			buffer.putInt(messageBatchSizes.mess[i]);
			this.serialize(message, buffer);
			i++;
		}
	}
	
	public byte[] serialize(StoredMessage message) throws SerializationException {
		ByteBuffer b = ByteBuffer.allocate(calculateMessageSize(message));
		this.serialize(message, b);
		return b.array();
	}

	public void serialize(StoredMessage message, ByteBuffer buffer) throws SerializationException {
		buffer.putShort(MESSAGE_MAGIC);
		this.printMessageId(message.getId(), buffer);
		printInstant(message.getTimestamp(), buffer);
		this.printMessageMetaData(message.getMetadata(), buffer);
		printBody(message.getContent(), buffer);
	}
	
	public int calculateMessageSize(StoredMessage message) {
		
		/* 
		 2 - magic number
		 2 - stream id length
		 1 - DIRECTION enum (ordinal)
		 8 - index (long)
		 4 + 8 = Instant (timestamp) long (seconds) + int (nanos)
		 4 - message body (byte[]) length
		 4 - metadata (map) length
		 
		 Collapsed constant = 33 
		 */

		int i = (message.getId() != null ? lenStr(message.getId().getStreamName()) : 0) 
			+ (message.getContent() != null ? message.getContent().length : 0) + 33;
		Map<String, String> md ;
		if (message.getMetadata() != null && (md = message.getMetadata().toMap()) != null) {
			for (Map.Entry<String, String> entry : md.entrySet()) {
				i += lenStr(entry.getKey())  // key
					+ lenStr(entry.getValue()) + 8; // value + 2 length
			}
		}
		return i;
	}

	private int lenStr(String str) {
		return str != null ? str.length() : 0;
	}

	public SerializationBatchSizes calculateMessageBatchSize(Collection<StoredMessage> message) {
		
		/* 
		 4 - magic number
		 1 - protocol version
		 4 - message sizes
		 Collapsed constant = 9
		 
		 every:
		 4 - message length
		 x - message
		  
		 */

		SerializationBatchSizes sizes = new SerializationBatchSizes(message.size());
		sizes.total = 9;
		
		int i  = 0;
		for (StoredMessage storedMessage : message) {
			sizes.mess[i] = this.calculateMessageSize(storedMessage);
			sizes.total += 4 + sizes.mess[i];
			i++;
		}
		
		return sizes;
	}
	
	private void printMessageId(StoredMessageId messageId, ByteBuffer buffer) throws SerializationException {
		printShortString(messageId.getStreamName(), buffer, "Session alias");
		buffer.put((byte) messageId.getDirection().ordinal());
		buffer.putLong(messageId.getIndex());
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
}
