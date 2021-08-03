/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import com.exactpro.cradle.messages.StoredMessageId;

public interface TestEventsMessagesLinker
{
	/**
	 * This method is deprecated, use {@link #getTestEventIds(StoredMessageId) getTestEventIds}
	 * Retrieves IDs of stored test events by linked message ID
	 * @param messageId ID of stored message
	 * @return collection of stored test event IDs
	 * @throws IOException if test event data retrieval failed
	 */
	@Deprecated
	Collection<StoredTestEventId> getTestEventIdsByMessageId(StoredMessageId messageId) throws IOException;
	
	/**
	 * This method is deprecated, use {@link #getTestEventIdsAsync(StoredMessageId) getTestEventIdsAsync}
	 * Asynchronously retrieves IDs of stored test events by linked message ID
	 * @param messageId ID of stored message
	 * @return future to obtain collection of stored test event IDs
	 */
	@Deprecated
	CompletableFuture<Collection<StoredTestEventId>> getTestEventIdsByMessageIdAsync(StoredMessageId messageId);
	
	/**
	 * Retrieves IDs of stored test events by linked message ID
	 * @param messageId ID of stored message
	 * @return collection of stored test event IDs
	 * @throws IOException if test event data retrieval failed
	 */
	Collection<ExtendedTestEventId> getTestEventIds(StoredMessageId messageId) throws IOException;
	
	/**
	 * Asynchronously retrieves IDs of stored test events by linked message ID
	 * @param messageId ID of stored message
	 * @return future to obtain collection of stored test event IDs
	 */
	CompletableFuture<Collection<ExtendedTestEventId>> getTestEventIdsAsync(StoredMessageId messageId);

	/**
	 * Retrieves IDs of stored messages by linked test event ID
	 * @param eventId ID of stored test event
	 * @return collection of stored message IDs
	 * @throws IOException if messages data retrieval failed
	 */
	Collection<StoredMessageId> getMessageIdsByTestEventId(StoredTestEventId eventId) throws IOException;
	
	/**
	 * Asynchronously retrieves IDs of stored messages by linked test event ID
	 * @param eventId ID of stored test event
	 * @return future to obtain collection of stored message IDs
	 */
	CompletableFuture<Collection<StoredMessageId>> getMessageIdsByTestEventIdAsync(StoredTestEventId eventId);

	/**
	 * Checks if test event has messages linked to it
	 * @param eventId ID of stored test event
	 * @return true if test event has linked messages, false otherwise
	 * @throws IOException if messages data retrieval failed
	 */
	boolean isTestEventLinkedToMessages(StoredTestEventId eventId) throws IOException;
	
	/**
	 * Asynchronously checks if test event has messages linked to it
	 * @param eventId ID of stored test event
	 * @return future to get if test event has linked messages
	 */
	CompletableFuture<Boolean> isTestEventLinkedToMessagesAsync(StoredTestEventId eventId);
	
	/**
	 * Checks if message has test events linked to it
	 * @param messageId ID of stored message
	 * @return true if message has linked test events, false otherwise
	 * @throws IOException if test events data retrieval failed
	 */
	boolean isMessageLinkedToTestEvents(StoredMessageId messageId) throws IOException;
	
	/**
	 * Asynchronously checks if message has test events linked to it
	 * @param messageId ID of stored message
	 * @return future to get if message has linked test events
	 */
	CompletableFuture<Boolean> isMessageLinkedToTestEventsAsync(StoredMessageId messageId);
}
