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
import com.exactpro.cradle.messages.CradleMessage;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import org.apache.commons.lang3.StringUtils;

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
	 * 	     4 - book name length
	 * 		 2 - stream id length
	 * 		 1 - DIRECTION enum (ordinal)
	 * 		 4 - message sizes
	 * 		 Collapsed constant = 16
	 *
	 * 		 every:
	 * 		 4 - message length
	 * 		 x - message
	 */
	public final static int MESSAGE_BATCH_CONST_VALUE = 16;
	
	public final static int MESSAGE_LENGTH_IN_BATCH = 4;

	public static int calculateMessageSize(CradleMessage message) {
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

	public static int calculateMessageSizeInBatch(CradleMessage message) {
		return calculateMessageSize(message) + MESSAGE_LENGTH_IN_BATCH;
	}

	private static int lenStr(String str) {
		return str != null ? str.length() : 0;
	}

	public static int calculateServiceMessageBatchSize(CradleMessage message) {
		int total = MESSAGE_BATCH_CONST_VALUE;
		StoredMessageId id;
		if (message != null && (id = message.getId()) != null) {
			if (StringUtils.isNotEmpty(id.getSessionAlias())) {
				total += message.getSessionAlias().length();
			}

			BookId bookId = id.getBookId();
			if (bookId != null && StringUtils.isNotEmpty(bookId.getName())) {
				total += bookId.getName().length();
			}
		}

		return total;
	}

	public static SerializationBatchSizes calculateMessageBatchSize(Collection<? extends CradleMessage> message) {

		SerializationBatchSizes sizes = new SerializationBatchSizes(message.size());
		sizes.total = calculateServiceMessageBatchSize(message.isEmpty() ? null : message.iterator().next());
		
		int i  = 0;
		for (CradleMessage storedMessage : message) {
			sizes.mess[i] = calculateMessageSize(storedMessage);
			sizes.total += MESSAGE_LENGTH_IN_BATCH + sizes.mess[i];
			i++;
		}

		return sizes;
	}
	
}
