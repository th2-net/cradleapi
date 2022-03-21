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

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.messages.StoredMessageMetadata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.*;
import static com.exactpro.cradle.serialization.SerializationUtils.*;

public class MessageSerializer {

	public SerializedEntityData serializeBatch(Collection<StoredMessage> batch) throws SerializationException {
		SerializationBatchSizes messageBatchSizes = MessagesSizeCalculator.calculateMessageBatchSize(batch);
		ByteBuffer buffer = ByteBuffer.allocate(messageBatchSizes.total);
		
		List<SerializedEntityMetadata> serializedMessageMetadata = this.serializeBatch(batch, buffer, messageBatchSizes);

		return new SerializedEntityData(serializedMessageMetadata, buffer.array());
	}

	public List<SerializedEntityMetadata> serializeBatch(
			Collection<StoredMessage> batch, ByteBuffer buffer, SerializationBatchSizes messageBatchSizes
	) throws SerializationException {

		if (messageBatchSizes == null) {
			messageBatchSizes = MessagesSizeCalculator.calculateMessageBatchSize(batch);
		}

		List<SerializedEntityMetadata> serializedMessageMetadata = new ArrayList<>(batch.size());

		buffer.putInt(MESSAGE_BATCH_MAGIC);
		buffer.put(MESSAGE_PROTOCOL_VER);
		
		buffer.putInt(batch.size());
		int i = 0;
		for (StoredMessage message : batch) {
			int messageSize = messageBatchSizes.entities[i];

			buffer.putInt(messageSize);
			this.serialize(message, buffer);
			serializedMessageMetadata.add(new SerializedEntityMetadata(message.getTimestamp(), messageSize));
			i++;
		}

		return serializedMessageMetadata;
	}
	
	public byte[] serialize(StoredMessage message) throws SerializationException {
		ByteBuffer b = ByteBuffer.allocate(MessagesSizeCalculator.calculateMessageSize(message));
		this.serialize(message, b);
		return b.array();
	}

	public void serialize(StoredMessage message, ByteBuffer buffer) throws SerializationException {
		buffer.putShort(MESSAGE_MAGIC);
		StoredMessageId id = message.getId();
		printInstant(id.getTimestamp(), buffer);
		buffer.putLong(id.getSequence());
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
