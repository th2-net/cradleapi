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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;

import java.time.Instant;

public interface CradleMessage
{
	/**
	 * @return ID of book the message is related to
	 */
	BookId getBookId();

	/**
	 * @return alias of session the message is related to
	 */
	String getSessionAlias();

	/**
	 * @return direction in which the message went through the session
	 */
	Direction getDirection();

	/**
	 * @return timestamp of message creation
	 */
	Instant getTimestamp();

	/**
	 * @return sequence number the message has for its session, direction and timestamp
	 */
	long getSequence();

	/**
	 * @return metadata attached to message
	 */
	StoredMessageMetadata getMetadata();

	/**
	 * @return message content
	 */
	byte[] getContent();
}
