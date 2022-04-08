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
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventWithContent;

import java.util.Collection;
import java.util.function.Function;

public class EventsSizeCalculator {

	/*
	2 - magic number
	4 - id length
	4 - name length
	4 - type length
	4 - parent id length
	4 + 8 = Instant (start timestamp) long (seconds) + int (nanos)
	4 + 8 = Instant (end timestamp) long (seconds) + int (nanos)
	1 = is success
	4 = body len
	
	===
	47
 	*/
	private static final int EVENT_RECORD_CONST = 47;

	/*
	2 - magic number
	4 - id length
	4 - name length
	4 - type length
	4 - parent id length
	4 + 8 = Instant (start timestamp) long (seconds) + int (nanos)
	4 + 8 = Instant (end timestamp) long (seconds) + int (nanos)
	1 = is success
	
	===
	43
	 */
	private static final int EVENT_METADATA_RECORD_CONST = 43;
	
	/*
		 4 - magic number
		 1 - protocol version
		 4 - message sizes
		 Collapsed constant = 9
	 */
	public static final int BATCH_LEN_CONST = 9;

	private final static int BATCH_LENGTH_IN_BATCH = 4;
	

	public static int calculateEventRecordSize(StoredTestEventWithContent message) {
		return EVENT_RECORD_CONST + lenId(message.getId()) + lenStr(message.getName()) + lenStr(message.getType())
				+ lenId(message.getParentId()) + (message.getContent() != null ? message.getContent().length : 0);

	}

	public static int calculateEventMetadataSize(BatchedStoredTestEventMetadata message) {
		return EVENT_METADATA_RECORD_CONST + lenId(message.getId()) + lenStr(message.getName()) + lenStr(message.getType())
				+ lenId(message.getParentId());
	}

	private static int lenId(StoredTestEventId id) {
		return id != null && id.getId() != null ? id.getId().length() : 0;
	}

	private static int lenStr(String str) {
		return str != null ? str.length() : 0;
	}

	public static <T> SerializationBatchSizes  calculateBatchEventMetadataSize(Collection<BatchedStoredTestEventMetadata> message) {
		return calculateAbstractBatchSize(message, EventsSizeCalculator::calculateEventMetadataSize);
	}

	public static <T> SerializationBatchSizes  calculateBatchEventSize(Collection<BatchedStoredTestEvent> message) {
		return calculateAbstractBatchSize(message, EventsSizeCalculator::calculateEventRecordSize);
	}


	private static <T> SerializationBatchSizes calculateAbstractBatchSize(Collection<T> message, Function<T, Integer> calcEnt) {
		
		SerializationBatchSizes sizes = new SerializationBatchSizes(message.size());
		sizes.total = BATCH_LEN_CONST;

		int i  = 0;
		for (T storedMessage : message) {
			sizes.mess[i] = calcEnt.apply(storedMessage);;
			sizes.total += 4 + sizes.mess[i];
			i++;
		}

		return sizes;
	}

	public static int calculateRecordSizeInBatch(StoredTestEventWithContent message) {
		return calculateEventRecordSize(message) + BATCH_LENGTH_IN_BATCH;
	}
	
}
