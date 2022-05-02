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

import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

public class MessagesSizeCalculator {

	/*
	 * 
	 * 	2 - magic number
	 * 	8 - index (long)
	 * 	4 + 8 = Instant (timestamp) long (seconds) + int (nanos)
	 * 	4 - message body (byte[]) length
	 * 	4 - metadata (map) length
	 *
	 * 	Collapsed constant = 30 
	 *  
	 */
	public final static int MESSAGE_SIZE_CONST_VALUE = 30;

	/*
	 * 	     4 - magic number
	 * 		 1 - protocol version
	 * 		 2 - stream id length
	 * 		 1 - DIRECTION enum (ordinal)
	 * 		 4 - message sizes
	 * 		 Collapsed constant = 12
	 *
	 * 		 every:
	 * 		 4 - message length
	 * 		 x - message
	 */
	public final static int MESSAGE_BATCH_CONST_VALUE = 12;
	
	public final static int MESSAGE_LENGTH_IN_BATCH = 4;

	public static int calculateMessageSize(StoredMessage message) {
		int i = (message.getContent() != null ? message.getContent().length : 0) + MESSAGE_SIZE_CONST_VALUE;
		Map<String, String> md ;
		if (message.getMetadata() != null && (md = message.getMetadata().toMap()) != null) {
			for (Map.Entry<String, String> entry : md.entrySet()) {
				i += lenStr(entry.getKey())  // key
						+ lenStr(entry.getValue()) + 8; // value + 2 length
			}
		}
		return i;
	}

	public static int calculateMessageSizeInBatch(StoredMessage message) {
		return calculateMessageSize(message) + MESSAGE_LENGTH_IN_BATCH;
	}

	public static int calculateMessageSize(MessageToStore message) {
		int i = (message.getContent() != null ? message.getContent().length : 0) + MESSAGE_SIZE_CONST_VALUE;
		Map<String, String> md ;
		if (message.getMetadata() != null && (md = message.getMetadata().toMap()) != null) {
			for (Map.Entry<String, String> entry : md.entrySet()) {
				i += lenStr(entry.getKey())  // key
						+ lenStr(entry.getValue()) + 8; // value + 2 length
			}
		}
		return i;
	}

	public static int calculateMessageSizeInBatch(MessageToStore message) {
		return calculateMessageSize(message) + MESSAGE_LENGTH_IN_BATCH;
	}

	private static int lenStr(String str) {
		return str != null ? str.getBytes(StandardCharsets.UTF_8).length : 0;
	}

	public static int calculateServiceMessageBatchSize(String streamName) {
		return (streamName != null ? streamName.length() : 0) + MESSAGE_BATCH_CONST_VALUE;
	}

	public static SerializationBatchSizes calculateMessageBatchSize(Collection<StoredMessage> message) {

		SerializationBatchSizes sizes = new SerializationBatchSizes(message.size());
		StoredMessageId msgId;
		String sessionAlias = (message.isEmpty() || ((msgId = message.iterator().next().getId()) == null)) ? null : msgId.getStreamName();
		sizes.total = calculateServiceMessageBatchSize(sessionAlias);
		
		int i  = 0;
		for (StoredMessage storedMessage : message) {
			sizes.mess[i] = calculateMessageSize(storedMessage);
			sizes.total += 4 + sizes.mess[i];
			i++;
		}

		return sizes;
	}
	
}
