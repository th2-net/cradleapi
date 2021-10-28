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
import java.util.Collection;

public interface MessageBatch
{
	StoredMessageId getId();

	/**
	 * @return alias of session all messages in the batch are related to
	 */
	String getSessionAlias();

	/**
	 * @return directions of messages in the batch
	 */
	Direction getDirection();

	/**
	 * @return number of messages currently stored in the batch
	 */
	int getMessageCount();

	/**
	 * @return size of messages currently stored in the batch
	 */
	int getBatchSize();

	/**
	 * @return collection of messages stored in the batch
	 */
	Collection<StoredMessage> getMessages();

	/**
	 * @return collection of messages stored in the batch in reverse order
	 */
	Collection<StoredMessage> getMessagesReverse();

	StoredMessage getFirstMessage();

	StoredMessage getLastMessage();
	
	/**
	 * @return timestamp of first message within the batch
	 */
	Instant getFirstTimestamp();

	/**
	 * @return timestamp of last message within the batch
	 */
	Instant getLastTimestamp();

	/**
	 * @return true if no messages were added to batch yet
	 */
	boolean isEmpty();

}
