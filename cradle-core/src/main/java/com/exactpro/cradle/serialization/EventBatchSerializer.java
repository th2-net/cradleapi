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
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Function;

import static com.exactpro.cradle.serialization.Serialization.EventBatchConst.*;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateEventRecordSize;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateEventMetadataSize;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateBatchEventSize;
import static com.exactpro.cradle.serialization.EventsSizeCalculator.calculateBatchEventMetadataSize;
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

	public byte[] serializeEventMetadataRecord (BatchedStoredTestEventMetadata message) throws SerializationException {
		ByteBuffer allocate = ByteBuffer.allocate(calculateEventMetadataSize(message));
		this.serializeEventMetadataRecord(message, allocate);
		return allocate.array();
	}
	
	public void serializeEventRecord (BatchedStoredTestEvent message, ByteBuffer buffer) throws SerializationException {
		buffer.putShort(EVENT_BATCH_ENT_MAGIC);
		printString(message.getId().getId(), buffer);
		printString(message.getName(), buffer);
		printString(message.getType(), buffer);
		printString(message.getParentId().getId(), buffer);
		printInstant(message.getStartTimestamp(), buffer);
		printInstant(message.getEndTimestamp(), buffer);
		printSingleBoolean(message.isSuccess(), buffer);
		printBody(message.getContent(), buffer);
	}

	public void serializeEventMetadataRecord (BatchedStoredTestEventMetadata metadata, ByteBuffer buffer) throws SerializationException {
		buffer.putShort(EVENT_METADATA_MAGIC);
		printString(metadata.getId().getId(), buffer);
		printString(metadata.getName(), buffer);
		printString(metadata.getType(), buffer);
		printString(metadata.getParentId().getId(), buffer);
		printInstant(metadata.getStartTimestamp(), buffer);
		printInstant(metadata.getEndTimestamp(), buffer);
		printSingleBoolean(metadata.isSuccess(), buffer);
	}

	public byte[] serializeEventBatch (Collection<BatchedStoredTestEvent> batch) throws SerializationException {
		SerializationBatchSizes sizes = calculateBatchEventSize(batch);
		ByteBuffer buffer = ByteBuffer.allocate(sizes.total);
		this.serializeAbstractBatch(EVENT_BATCH_MAGIC, EVENT_BATCH_PROTOCOL_VER, batch, buffer, sizes,
				this::serializeEventRecord);
		return buffer.array();
	}

	public byte[] serializeEventMetadataBatch (Collection<BatchedStoredTestEventMetadata> batch) throws SerializationException {
		SerializationBatchSizes sizes = calculateBatchEventMetadataSize(batch);
		ByteBuffer buffer = ByteBuffer.allocate(sizes.total);
		this.serializeAbstractBatch(EVENT_BATCH_MD_MAGIC, EVENT_BATCH_MD_PROTOCOL_VER, batch, buffer,
				calculateBatchEventMetadataSize(batch), this::serializeEventMetadataRecord);
		return buffer.array();
	}
	
	public void serializeEventBatch (Collection<BatchedStoredTestEvent> batch, ByteBuffer buffer) throws SerializationException {
		this.serializeAbstractBatch(EVENT_BATCH_MAGIC, EVENT_BATCH_PROTOCOL_VER, batch, buffer,
				calculateBatchEventSize(batch), this::serializeEventRecord);
	}

	public void serializeEventMetadataBatch (Collection<BatchedStoredTestEventMetadata> batch, ByteBuffer buffer) throws SerializationException {
		this.serializeAbstractBatch(EVENT_BATCH_MD_MAGIC, EVENT_BATCH_MD_PROTOCOL_VER, batch, buffer,
				calculateBatchEventMetadataSize(batch), this::serializeEventMetadataRecord);
	}

	private <T> void serializeAbstractBatch (int magic, byte protVer, Collection<T> batch, ByteBuffer buffer,
						 SerializationBatchSizes messageBatchSizes, SerializeFunc<T> serialize) throws SerializationException {
		buffer.putInt(magic);
		buffer.put(protVer);
		buffer.putInt(batch.size());
		int i = 0;
		for (T message : batch) {
			buffer.putInt(messageBatchSizes.mess[i]);
			serialize.serialize(message, buffer);
			i++;
		}
	}

	@FunctionalInterface
	public interface SerializeFunc<T> {
		void serialize(T t, ByteBuffer u) throws SerializationException;
	}
	
}
