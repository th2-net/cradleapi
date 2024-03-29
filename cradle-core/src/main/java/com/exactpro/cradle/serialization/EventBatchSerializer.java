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

import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateBatchEventSize;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateEventRecordSize;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_ENT_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_MAGIC;
import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.EVENT_BATCH_PROTOCOL_VER;
import static com.exactpro.cradle.serialization.SerializationUtils.printBody;
import static com.exactpro.cradle.serialization.SerializationUtils.printInstant;
import static com.exactpro.cradle.serialization.SerializationUtils.printSingleBoolean;
import static com.exactpro.cradle.serialization.SerializationUtils.printString;

public class EventBatchSerializer {


	public byte[] serializeEventRecord(BatchedStoredTestEvent event) {
		ByteBuffer allocate = ByteBuffer.allocate(calculateEventRecordSize(event));
		this.serializeEventRecord(event, allocate);
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

	public void serializeEventRecord(BatchedStoredTestEvent event, ByteBuffer buffer) {
		buffer.putShort(EVENT_BATCH_ENT_MAGIC);

		printId(event.getId(), buffer);
		printString(event.getName(), buffer);
		printString(event.getType(), buffer);
		printId(event.getParentId(), buffer);
		printInstant(event.getEndTimestamp(), buffer);
		printSingleBoolean(event.isSuccess(), buffer);
		printBody(event.getContent(), buffer);
	}


	public SerializedEntityData<SerializedEntityMetadata> serializeEventBatch(Collection<BatchedStoredTestEvent> batch) {
		SerializationBatchSizes sizes = calculateBatchEventSize(batch);
		ByteBuffer buffer = ByteBuffer.allocate(sizes.total);
		List<SerializedEntityMetadata> serializedEventMetadata = serializeEventBatch(batch, buffer, sizes);

		return new SerializedEntityData<>(serializedEventMetadata, buffer.array());
	}

	public void serializeEventBatch(Collection<BatchedStoredTestEvent> batch, ByteBuffer buffer) {
		SerializationBatchSizes eventBatchSizes = calculateBatchEventSize(batch);
		serializeEventBatch(batch, buffer, eventBatchSizes);
	}

	public List<SerializedEntityMetadata> serializeEventBatch(
			Collection<BatchedStoredTestEvent> batch, ByteBuffer buffer, SerializationBatchSizes eventBatchSizes
	) {

		List<SerializedEntityMetadata> serializedEventMetadata = new ArrayList<>(batch.size());

		buffer.putInt(EVENT_BATCH_MAGIC);
		buffer.put(EVENT_BATCH_PROTOCOL_VER);

		buffer.putInt(batch.size());
		int i = 0;
		for (BatchedStoredTestEvent event : batch) {
			int eventSize = eventBatchSizes.entities[i];

			buffer.putInt(eventSize);
			this.serializeEventRecord(event, buffer);
			serializedEventMetadata.add(new SerializedEntityMetadata(event.getStartTimestamp(), eventSize));
			i++;
		}

		return Collections.unmodifiableList(serializedEventMetadata);
	}

	public SerializedEntityData<SerializedEntityMetadata> serializeEvent(TestEventSingleToStore testEvent) {
		byte[] eventContent = testEvent.getContent();
		SerializedEntityMetadata serializedEventMetadata = new SerializedEntityMetadata(
				testEvent.getStartTimestamp(), eventContent == null ? 0 : eventContent.length
		);

		return new SerializedEntityData<>(Collections.singletonList(serializedEventMetadata), eventContent);
	}
}
