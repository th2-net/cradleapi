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

import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.BatchedStoredTestEventBuilder;
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata;
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadataBuilder;
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_ENT_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_MD_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_MD_PROTOCOL_VER;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_PROTOCOL_VER;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_METADATA_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.INVALID_MAGIC_NUMBER_FORMAT;
import static com.exactpro.cradle.serialization.Serialization.NOT_SUPPORTED_PROTOCOL_FORMAT;
import static com.exactpro.cradle.serialization.SerializationUtils.readBody;
import static com.exactpro.cradle.serialization.SerializationUtils.readInstant;
import static com.exactpro.cradle.serialization.SerializationUtils.readSingleBoolean;
import static com.exactpro.cradle.serialization.SerializationUtils.readString;

public class EventBatchDeserializer {
	
	public boolean checkEventBatchHeader(byte[] array) {
		return ByteBuffer.wrap(array, 0, 4).getInt() == EVENT_BATCH_MAGIC;
	}

	public boolean checkEventBatchMetadataHeader(byte[] array) {
		return ByteBuffer.wrap(array, 0, 4).getInt() == EVENT_BATCH_MD_MAGIC;
	}

	public void deserializeBatchEntries(byte[] buffer, SerializationConsumer<BatchedStoredTestEvent> action)
			throws Exception {
		deserializeBatchEntries(ByteBuffer.wrap(buffer), action);
	}

	public void deserializeBatchEntries(ByteBuffer buffer, SerializationConsumer<BatchedStoredTestEvent> action)
			throws Exception {
		int magicNumber = buffer.getInt();
		if (magicNumber != EVENT_BATCH_MAGIC) {
			throw SerializationUtils.incorrectMagicNumber("EVENT_BATCH", magicNumber, EVENT_BATCH_MAGIC);
		}

		byte protocolVer = buffer.get();
		if (protocolVer != EVENT_BATCH_PROTOCOL_VER) {
			throw new SerializationException(String.format(NOT_SUPPORTED_PROTOCOL_FORMAT, "event batches",
					protocolVer, EVENT_BATCH_PROTOCOL_VER));
		}

		int batchesCount = buffer.getInt();
		for (int i = 0; i < batchesCount; ++i) {
			int msgLen = buffer.getInt();
			ByteBuffer msgBuf = ByteBuffer.wrap(buffer.array(), buffer.position(), msgLen);
			BatchedStoredTestEvent batchedStoredTestEvent = this.deserializeBatchEntry(msgBuf);
			buffer.position(buffer.position() + msgLen);
			action.accept(batchedStoredTestEvent);
		}
	}

	public void deserializeBatchEntriesMetadata(byte[] buffer, SerializationConsumer<BatchedStoredTestEventMetadata> action)
			throws Exception {
		deserializeBatchEntriesMetadata(ByteBuffer.wrap(buffer), action);
	}

	public void deserializeBatchEntriesMetadata(ByteBuffer buffer, SerializationConsumer<BatchedStoredTestEventMetadata> action)
			throws Exception {
		int magicNumber = buffer.getInt();
		if (magicNumber != EVENT_BATCH_MD_MAGIC) {
			throw SerializationUtils.incorrectMagicNumber("EVENT_BATCH_METADATA", magicNumber, EVENT_BATCH_MAGIC);
		}

		byte protocolVer = buffer.get();
		if (protocolVer != EVENT_BATCH_MD_PROTOCOL_VER) {
			throw new SerializationException(String.format(NOT_SUPPORTED_PROTOCOL_FORMAT, "event batches metadata",
					protocolVer, EVENT_BATCH_MD_PROTOCOL_VER));
		}

		int batchMdCount = buffer.getInt();
		for (int i = 0; i < batchMdCount; ++i) {
			int msgLen = buffer.getInt();
			ByteBuffer msgBuf = ByteBuffer.wrap(buffer.array(), buffer.position(), msgLen);
			BatchedStoredTestEventMetadata batchedStoredTestEvent = this.deserializeBatchEntryMetadata(msgBuf);
			buffer.position(buffer.position() + msgLen);
			action.accept(batchedStoredTestEvent);
		}
	}

	public List<BatchedStoredTestEvent> deserializeBatchEntries(ByteBuffer buffer) throws Exception {
		final ArrayList<BatchedStoredTestEvent> batches = new ArrayList<>();
		deserializeBatchEntries(buffer, batches::add);
		return batches;
	}

	public List<BatchedStoredTestEvent> deserializeBatchEntries(byte[] buffer) throws Exception {
		return deserializeBatchEntries(ByteBuffer.wrap(buffer));
	}

	public List<BatchedStoredTestEventMetadata> deserializeBatchEntriesMetadata(ByteBuffer buffer) throws Exception {
		final ArrayList<BatchedStoredTestEventMetadata> batches = new ArrayList<>();
		deserializeBatchEntriesMetadata(buffer, batches::add);
		return batches;
	}
	
	public List<BatchedStoredTestEventMetadata> deserializeBatchEntriesMetadata(byte[] buffer) throws Exception {
		return deserializeBatchEntriesMetadata(ByteBuffer.wrap(buffer));
	}

	public BatchedStoredTestEvent deserializeBatchEntry(byte[] buffer) throws SerializationException {
		return this.deserializeBatchEntry(ByteBuffer.wrap(buffer));
	}

	public BatchedStoredTestEventMetadata deserializeBatchEntryMetadata(byte[] buffer) throws SerializationException {
		return this.deserializeBatchEntryMetadata(ByteBuffer.wrap(buffer));
	}

	public BatchedStoredTestEvent deserializeBatchEntry(ByteBuffer buffer) throws SerializationException {
		short magicNumber = buffer.getShort();
		if (magicNumber != EVENT_BATCH_ENT_MAGIC) {
			throw SerializationUtils.incorrectMagicNumber(BatchedStoredTestEvent.class.getSimpleName(), magicNumber, EVENT_BATCH_ENT_MAGIC);
		}

		BatchedStoredTestEventBuilder eventBuilder = new BatchedStoredTestEventBuilder();

		eventBuilder.setId(new StoredTestEventId(readString(buffer)));
		eventBuilder.setName(readString(buffer));
		eventBuilder.setType(readString(buffer));
		eventBuilder.setParentId(new StoredTestEventId(readString(buffer)));
		eventBuilder.setStartTimestamp(readInstant(buffer));
		eventBuilder.setEndTimestamp(readInstant(buffer));
		eventBuilder.setSuccess(readSingleBoolean(buffer));
		eventBuilder.setContent(readBody(buffer));
		
		return eventBuilder.build();
	}

	public BatchedStoredTestEventMetadata deserializeBatchEntryMetadata(ByteBuffer buffer) throws SerializationException {
		long magicNumber = buffer.getShort();
		if (magicNumber != EVENT_METADATA_MAGIC) {
			throw new SerializationException(String.format(INVALID_MAGIC_NUMBER_FORMAT,
					BatchedStoredTestEventMetadata.class.getSimpleName(), magicNumber, EVENT_METADATA_MAGIC));
		}

		BatchedStoredTestEventMetadataBuilder eventBuilder = new BatchedStoredTestEventMetadataBuilder();

		eventBuilder.setId(new StoredTestEventId(readString(buffer)));
		eventBuilder.setName(readString(buffer));
		eventBuilder.setType(readString(buffer));
		eventBuilder.setParentId(new StoredTestEventId(readString(buffer)));
		eventBuilder.setStartTimestamp(readInstant(buffer));
		eventBuilder.setEndTimestamp(readInstant(buffer));
		eventBuilder.setSuccess(readSingleBoolean(buffer));

		return eventBuilder.build();
	}
	
}
