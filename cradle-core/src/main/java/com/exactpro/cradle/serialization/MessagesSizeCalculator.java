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

import com.exactpro.cradle.messages.CradleMessage;

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
	 *  4 - magic number
	 * 	1 - protocol version
	 * 	4 - message sizes
	 * 	Collapsed constant = 9
	 *
	 *  every:
	 *  4 - message length
	 * 	x - message
	 */
	public final static int MESSAGE_BATCH_CONST_VALUE = 9;
	
	public final static int MESSAGE_LENGTH_IN_BATCH = 4;

	public static int calculateMessageSize(CradleMessage message) {
		int len = (message.getContent() != null ? message.getContent().length : 0) + MESSAGE_SIZE_CONST_VALUE;
		len += calculateStringSize(message.getSessionAlias()) +
				calculateStringSize(message.getDirection().getLabel()) +
				calculateStringSize(message.getProtocol());
		Map<String, String> md ;
		if (message.getMetadata() != null && (md = message.getMetadata().toMap()) != null) {
			for (Map.Entry<String, String> entry : md.entrySet()) {
				len += calculateStringSize(entry.getKey())  // key
						+ calculateStringSize(entry.getValue()); // value
			}
		}
		return len;
	}

	public static int calculateMessageSizeInBatch(CradleMessage message) {
		return calculateMessageSize(message) + MESSAGE_LENGTH_IN_BATCH;
	}

	public static int calculateStringSize(String str) {
		return (str != null ? str.getBytes(StandardCharsets.UTF_8).length : 0) + 4;
	}

	public static int calculateMessageBatchSize(Collection<? extends CradleMessage> message) {
		var total = MESSAGE_BATCH_CONST_VALUE;

		for (CradleMessage storedMessage : message) {
			total += storedMessage.getSerializedSize();
		}

		return total;
	}
}
