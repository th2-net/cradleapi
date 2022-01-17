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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;

import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.*;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateEventRecordSize;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateBatchEventSize;
import static com.exactpro.cradle.serialization.SerializationUtils.printBody;
import static com.exactpro.cradle.serialization.SerializationUtils.printInstant;
import static com.exactpro.cradle.serialization.SerializationUtils.printSingleBoolean;
import static com.exactpro.cradle.serialization.SerializationUtils.printString;

public class EventBatchSerializer {


	public byte[] serializeEventRecord (BatchedStoredTestEvent message) throws SerializationException {
		ByteBuffer allocate = ByteBuffer.allocate(calculateEventRecordSize(message));
		this.serializeEventRecord(message, allocate);
		return allocate.array();
	}

	private void printId(StoredTestEventId id, ByteBuffer buffer) {
		Instant start = null;
		String id_str = null;
		if (id != null) {
			start = id.getStartTimestamp();
			id_str = id.getId();
		}
		printInstant(start, buffer);
		printString(id_str, buffer);
	}
	
	public void serializeEventRecord (BatchedStoredTestEvent message, ByteBuffer buffer) throws SerializationException {
		buffer.putShort(EVENT_BATCH_ENT_MAGIC);

		printId(message.getId(), buffer);
		printString(message.getName(), buffer);
		printString(message.getType(), buffer);
		printId(message.getParentId(), buffer);
		printInstant(message.getEndTimestamp(), buffer);
		printSingleBoolean(message.isSuccess(), buffer);
		printBody(message.getContent(), buffer);
	}


	public byte[] serializeEventBatch (Collection<BatchedStoredTestEvent> batch) throws SerializationException {
		SerializationBatchSizes sizes = calculateBatchEventSize(batch);
		ByteBuffer buffer = ByteBuffer.allocate(sizes.total);
		serializeEventBatch(batch, buffer, sizes);
		return buffer.array();
	}

	public void serializeEventBatch (Collection<BatchedStoredTestEvent> batch, ByteBuffer buffer) throws SerializationException {
		SerializationBatchSizes messageBatchSizes = calculateBatchEventSize(batch);
		serializeEventBatch(batch, buffer, messageBatchSizes);
	}

	public void serializeEventBatch (Collection<BatchedStoredTestEvent> batch, ByteBuffer buffer,
									 SerializationBatchSizes messageBatchSizes) throws SerializationException {

		buffer.putInt(EVENT_BATCH_MAGIC);
		buffer.put(EVENT_BATCH_PROTOCOL_VER);

		EventBatchCommonParams commonParams = getCommonParams(batch);
		printString(commonParams.getBookName(), buffer);
		printString(commonParams.getScope(), buffer);

		buffer.putInt(batch.size());
		int i = 0;
		for (BatchedStoredTestEvent message : batch) {
			buffer.putInt(messageBatchSizes.mess[i]);
			this.serializeEventRecord(message, buffer);
			i++;
		}
	}

	private EventBatchCommonParams getCommonParams(Collection<BatchedStoredTestEvent> batch) {
		Iterator<BatchedStoredTestEvent> iterator = batch.iterator();
		if (iterator.hasNext()) {
			EventBatchCommonParams params = new EventBatchCommonParams();
			BatchedStoredTestEvent event = iterator.next();

			StoredTestEventId eventId = event.getId();
			BookId bookId = eventId != null ? eventId.getBookId() : null;

			params.setBookName(bookId != null ? bookId.getName() : null);
			params.setScope(eventId != null ? eventId.getScope() : null);
			return params;
		} else {
			return new EventBatchCommonParams();
		}
	}
}
