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
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.messages.StoredMessageMetadata;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.*;
import static com.exactpro.cradle.serialization.SerializationUtils.printBody;
import static com.exactpro.cradle.serialization.SerializationUtils.printInstant;
import static com.exactpro.cradle.serialization.SerializationUtils.printString;

public class MessageSerializer {

	public byte[] serializeBatch(Collection<StoredMessage> batch) throws SerializationException {
		SerializationBatchSizes messageBatchSizes = MessagesSizeCalculator.calculateMessageBatchSize(batch);
		ByteBuffer buffer = ByteBuffer.allocate(messageBatchSizes.total);
		
		this.serializeBatch(batch, buffer, messageBatchSizes);
		
		return buffer.array();
	}

	public void serializeBatch(Collection<StoredMessage> batch, ByteBuffer buffer, SerializationBatchSizes messageBatchSizes) throws SerializationException {

		if (messageBatchSizes == null) {
			messageBatchSizes = MessagesSizeCalculator.calculateMessageBatchSize(batch);
		}
		
		buffer.putInt(MESSAGE_BATCH_MAGIC);
		buffer.put(MESSAGE_PROTOCOL_VER);

		StoredMessageId msgId = batch.isEmpty() ? new StoredMessageId("", Direction.FIRST, 0L) 
				: batch.iterator().next().getId();
		SerializationUtils.printShortString(msgId.getStreamName(), buffer, "Session alias");
		buffer.put((byte) msgId.getDirection().ordinal());
		
		buffer.putInt(batch.size());
		int i = 0;
		for (StoredMessage message : batch) {
			buffer.putInt(messageBatchSizes.mess[i]);
			this.serialize(message, buffer);
			i++;
		}
	}
	
	public byte[] serialize(StoredMessage message) throws SerializationException {
		ByteBuffer b = ByteBuffer.allocate(MessagesSizeCalculator.calculateMessageSize(message));
		this.serialize(message, b);
		return b.array();
	}

	public void serialize(StoredMessage message, ByteBuffer buffer) throws SerializationException {
		buffer.putShort(MESSAGE_MAGIC);
		buffer.putLong(message.getId().getIndex());
		printInstant(message.getTimestamp(), buffer);
		this.printMessageMetaData(message.getMetadata(), buffer);
		printBody(message.getContent(), buffer);
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
