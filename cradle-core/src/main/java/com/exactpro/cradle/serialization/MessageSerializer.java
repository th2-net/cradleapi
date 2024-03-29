/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.messages.StoredMessageMetadata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.exactpro.cradle.serialization.MessagesSizeCalculator.MESSAGE_LENGTH_IN_BATCH;
import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.MESSAGE_BATCH_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.MESSAGE_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.MessageBatchConst.MESSAGE_PROTOCOL_VER;
import static com.exactpro.cradle.serialization.SerializationUtils.printBody;
import static com.exactpro.cradle.serialization.SerializationUtils.printInstant;
import static com.exactpro.cradle.serialization.SerializationUtils.printString;

public class MessageSerializer {
	public SerializedEntityData<SerializedMessageMetadata> serializeBatch(MessageBatchToStore batch) {
		return serializeBatch(batch.getMessages(), batch.getBatchSize());
	}

	public SerializedEntityData<SerializedMessageMetadata> serializeBatch(GroupedMessageBatchToStore batch) {
		return serializeBatch(batch.getMessages(), batch.getBatchSize());
	}

	public SerializedEntityData<SerializedMessageMetadata> serializeBatch(Collection<StoredMessage> batch) {
		return serializeBatch(batch, MessagesSizeCalculator.calculateMessageBatchSize(batch));
	}

	private SerializedEntityData<SerializedMessageMetadata> serializeBatch(Collection<StoredMessage> batch, int batchSize) {
		ByteBuffer buffer = ByteBuffer.allocate(batchSize);
		List<SerializedMessageMetadata> serializedMessageMetadata = this.serializeBatch(batch, buffer);
		return new SerializedEntityData<>(serializedMessageMetadata, buffer.array());
	}

	public List<SerializedMessageMetadata> serializeBatch(
			Collection<StoredMessage> batch, ByteBuffer buffer
	) {
		List<SerializedMessageMetadata> serializedMessageMetadata = new ArrayList<>(batch.size());

		buffer.putInt(MESSAGE_BATCH_MAGIC);
		buffer.put(MESSAGE_PROTOCOL_VER);

		buffer.putInt(batch.size());
		for (StoredMessage message : batch) {
			int messageSize = message.getSerializedSize() - MESSAGE_LENGTH_IN_BATCH;
			buffer.putInt(messageSize);
			this.serialize(message, buffer);
			serializedMessageMetadata.add(new SerializedMessageMetadata(message.getSessionAlias(), message.getDirection(), message.getTimestamp(), messageSize));
		}

		return serializedMessageMetadata;
	}

	public byte[] serialize(StoredMessage message) {
		ByteBuffer b = ByteBuffer.allocate(MessagesSizeCalculator.calculateMessageSize(message));
		this.serialize(message, b);
		return b.array();
	}

	public void serialize(StoredMessage message, ByteBuffer buffer) {
		buffer.putShort(MESSAGE_MAGIC);
		StoredMessageId id = message.getId();
		printString(id.getSessionAlias(), buffer);
		printString(id.getDirection().getLabel(), buffer);
		printInstant(id.getTimestamp(), buffer);
		buffer.putLong(id.getSequence());
		printString(message.getProtocol(), buffer);
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
