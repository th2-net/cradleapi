/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingle;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static com.exactpro.cradle.testevents.StoredTestEventIdUtils.logId;

public class EventsSizeCalculator {

	/*
		2 - magic number
		4 + 8 = Instant (start timestamp) long (seconds) + int (nanos) - start timestamp ID
		4 - id length
		4 - name length
		4 - type length
		4 + 8 = Instant (start timestamp) long (seconds) + int (nanos) - start timestamp parent ID
		4 - parent id length
		4 + 8 = Instant (end timestamp) long (seconds) + int (nanos)
		1 = is success
		4 = body len

		===
		59
 	*/
	private static final int EVENT_RECORD_CONST = 59;

	
	/*
		4 - magic number
		1 - protocol version
		4 - message sizes
		Collapsed constant = 9
	 */
	public static final int EVENT_BATCH_LEN_CONST = 9;

	private final static int ENTITY_LENGTH_IN_BATCH = 4;
	

	public static int calculateEventRecordSize(TestEventSingle event) {
		return EVENT_RECORD_CONST + lenId(event.getId()) + lenStr(event.getName()) + lenStr(event.getType())
				+ lenId(event.getParentId()) + (event.getContent() != null ? event.getContent().length : 0);

	}

	public static int getEventRecordSize(TestEventSingle event) {
		if (event.getSize() > 0) {
			return event.getSize();
		}
		throw new IllegalStateException("Event '" + logId(event) + "' isn't prepared for store");
	}


	private static int lenId(StoredTestEventId id) {
		return id != null ? lenStr(id.getId()) : 0;
	}

	private static int lenStr(String str) {
		return str != null ? str.getBytes(StandardCharsets.UTF_8).length : 0;
	}

	public static SerializationBatchSizes calculateBatchEventSize(Collection<BatchedStoredTestEvent> events) {

		SerializationBatchSizes sizes = new SerializationBatchSizes(events.size());
		sizes.total = EVENT_BATCH_LEN_CONST;

		int i  = 0;
		for (BatchedStoredTestEvent storedEvent : events) {
			sizes.entities[i] = getEventRecordSize(storedEvent);
			sizes.total += ENTITY_LENGTH_IN_BATCH + sizes.entities[i];
			i++;
		}

		return sizes;
	}

	public static SerializationBatchSizes getBatchEventSize(TestEventBatchToStore batch) {
		return new SerializationBatchSizes(batch.getBatchSize(), batch.getTestEvents().stream()
				.mapToInt(EventsSizeCalculator::getRecordSizeInBatch)
				.toArray());
	}

	public static int calculateRecordSizeInBatch(TestEventSingle event) {
		return calculateEventRecordSize(event) + ENTITY_LENGTH_IN_BATCH;
	}

	public static int getRecordSizeInBatch(TestEventSingle event) {
		if (event.getSize() > 0) {
			return event.getSize() + ENTITY_LENGTH_IN_BATCH;
		}
		throw new IllegalStateException("Event '" + logId(event) + "' isn't prepared for store");
	}
}
