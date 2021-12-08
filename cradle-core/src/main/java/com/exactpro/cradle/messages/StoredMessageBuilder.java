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

package com.exactpro.cradle.messages;

import com.exactpro.cradle.Direction;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class StoredMessageBuilder {

	//id
	private String streamName;
	private Direction direction;
	private long index;
	
	private StoredMessageId messageId;
	
	//metadata
	protected Map<String, String> metadata;
	
	private Instant timestamp;
	private byte[] content;

	public StoredMessageBuilder setStreamName(String streamName) {
		this.streamName = streamName;
		return this;
	}

	public StoredMessageBuilder setDirection(Direction direction) {
		this.direction = direction;
		return this;
	}

	public StoredMessageBuilder setIndex(long index) {
		this.index = index;
		return this;
	}

	public StoredMessageBuilder setMessageId(StoredMessageId messageId) {
		this.messageId = messageId;
		return this;
	}

	public StoredMessageBuilder putMetadata(String key, String value) {
		if (metadata == null) {
			this.metadata = new HashMap<>();
		}
		this.metadata.put(key, value);
		return this;
	}

	public StoredMessageBuilder setTimestamp(Instant timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	public StoredMessageBuilder setContent(byte[] content) {
		this.content = content;
		return this;
	}
	
	public StoredMessage build() {
		StoredMessageMetadata smm = metadata == null ? StoredMessageMetadata.empty() 
				: new StoredMessageMetadata(this.metadata);
		StoredMessageId msgId = messageId;
		if (msgId == null) {
			msgId = new StoredMessageId(streamName, direction, index);
		}
		return new StoredMessage(msgId, timestamp, smm, content);
	}
}
