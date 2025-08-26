/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_ENT_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_PROTOCOL_VER;
import static com.exactpro.cradle.serialization.Serialization.NOT_SUPPORTED_PROTOCOL_FORMAT;
import static com.exactpro.cradle.serialization.SerializationUtils.*;

public class EventBatchDeserializer {
	
	public boolean checkEventBatchHeader(byte[] array) {
		return ByteBuffer.wrap(array, 0, 4).getInt() == EVENT_BATCH_MAGIC;
	}

	public List<BatchedStoredTestEvent> deserializeBatchEntries(byte[] buffer, EventBatchCommonParams common)
			throws SerializationException {
		return deserializeBatchEntries(ByteBuffer.wrap(buffer), common);
	}

	public List<BatchedStoredTestEvent> deserializeBatchEntries(ByteBuffer buffer, EventBatchCommonParams common)
			throws SerializationException {
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
		List<BatchedStoredTestEvent> eventList = new ArrayList<>(batchesCount);
		for (int i = 0; i < batchesCount; ++i) {
			int msgLen = buffer.getInt();
			ByteBuffer msgBuf = ByteBuffer.wrap(buffer.array(), buffer.position(), msgLen);
			BatchedStoredTestEvent batchedStoredTestEvent = this.deserializeBatchEntry(msgBuf, common);
			buffer.position(buffer.position() + msgLen);
			eventList.add(batchedStoredTestEvent);
		}
		return eventList;
	}

	public BatchedStoredTestEvent deserializeBatchEntry(byte[] buffer, EventBatchCommonParams common) throws SerializationException {
		return this.deserializeBatchEntry(ByteBuffer.wrap(buffer), common);
	}

	private StoredTestEventId readId(EventBatchCommonParams common, ByteBuffer buffer) throws SerializationException {
		Instant start = readInstant(buffer);
		String id_str = readString(buffer);
		if (start == null && id_str == null) {
			return null;
		}
		return new StoredTestEventId(common.getBookId(), common.getScope(), start, id_str);
	}

	public BatchedStoredTestEvent deserializeBatchEntry(ByteBuffer buffer, EventBatchCommonParams common) throws SerializationException {
		short magicNumber = buffer.getShort();
		if (magicNumber != EVENT_BATCH_ENT_MAGIC) {
			throw SerializationUtils.incorrectMagicNumber(BatchedStoredTestEvent.class.getSimpleName(), magicNumber, EVENT_BATCH_ENT_MAGIC);
		}

		BatchedStoredTestEventBuilder eventBuilder = new BatchedStoredTestEventBuilder();

		eventBuilder.setId(readId(common, buffer));
		eventBuilder.setName(readString(buffer));
		eventBuilder.setType(readString(buffer));
		eventBuilder.setParentId(readId(common, buffer));
		eventBuilder.setEndTimestamp(readInstant(buffer));
		eventBuilder.setSuccess(readSingleBoolean(buffer));
		eventBuilder.setContent(readBody(buffer));

		return eventBuilder.build();
	}
}
