/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class StoredGroupMessageBatch extends AbstractStoredMessageBatch
{
	private static final Logger logger = LoggerFactory.getLogger(StoredGroupMessageBatch.class);

	private static class StoredMessageKey {
		private final String stream;
		private final Direction direction;

		public StoredMessageKey(String stream, Direction direction) {
			this.stream = stream;
			this.direction = direction;
		}

		public String getStream() {
			return stream;
		}

		public Direction getDirection() {
			return direction;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			StoredMessageKey that = (StoredMessageKey) o;
			return Objects.equals(getStream(), that.getStream()) && getDirection() == that.getDirection();
		}

		@Override
		public int hashCode() {
			return Objects.hash(getStream(), getDirection());
		}
	}

	private final Map<StoredMessageKey, MessageToStore> storedMessageSequences;

	public StoredGroupMessageBatch(StoredMessageBatch storedMessageBatch) {
		this();

		try {
			for (StoredMessage message : storedMessageBatch.getMessages()) {
				MessageToStoreBuilder builder = new MessageToStoreBuilder()
						.content(message.getContent())
						.direction(message.getDirection())
						.streamName(message.getStreamName())
						.timestamp(message.getTimestamp())
						.index(message.getIndex());
				StoredMessageMetadata metadata = message.getMetadata();
				if (metadata != null) {
					metadata.toMap().forEach(builder::metadata);
				}

				this.addMessage(builder.build());

			}
		} catch (CradleStorageException e) {
			logger.error("Could not create group batch from batch {}: {}",
					storedMessageBatch.getId(), e.getMessage());
		} catch (Exception e) {
			logger.error("Could not create group batch from batch");
		}
	}

	public StoredGroupMessageBatch()
	{
		super();
		this.storedMessageSequences = new HashMap<>();
	}

	public StoredGroupMessageBatch(long maxBatchSize)
	{
		super (maxBatchSize);
		this.storedMessageSequences = new HashMap<>();
	}

	@Override
	protected StoredMessage addMessageInternal(MessageToStore message, int expectedMessageSize) {
		return null;
	}

	@Override
	protected int calculateSizeAndCheckConstraints(MessageToStore message) throws CradleStorageException {
		int expectedMessageSize = MessagesSizeCalculator.calculateMessageSizeInGroupBatch(message);
		if (!hasSpace(expectedMessageSize, message.getStreamName()))
			throw new CradleStorageException("Batch has not enough space to hold given message");

		MessageUtils.validateMessage(message);

		String sm = message.getStreamName();
		Direction d = message.getDirection();
		long i = message.getIndex();
		if (StringUtils.isEmpty(sm))
			throw new CradleStorageException("Stream name cannot be empty");
		if (d == null)
			throw new CradleStorageException("Message direction must be set");
		if (i < 0)
			throw new CradleStorageException("Message index cannot be negative");

		MessageToStore lastMessageInCategory = storedMessageSequences.get(new StoredMessageKey(message.getStreamName(), message.getDirection()));
		if (lastMessageInCategory != null) {
			if (message.getIndex() <= lastMessageInCategory.getIndex()) {
				throw new CradleStorageException("Message index should be greater than "+lastMessageInCategory.getIndex()+
						" for the batch to contain sequenced messages, but in your message it is "+message.getIndex());
			}

			if (lastMessageInCategory.getIndex() + 1 != message.getIndex()) {
				logger.debug("Message index should be "+(lastMessageInCategory.getIndex()+1)+
						" for the batch to contain strictly sequenced messages, but in your message it is "+message.getIndex());
			}

			if (lastMessageInCategory.getTimestamp().isAfter(message.getTimestamp())) {
				throw new CradleStorageException(
						"Message timestamp should be not less than last message timestamp in batch '"
								+ lastMessageInCategory.getTimestamp() + "' but in your message it is '" + message.getTimestamp() + "'");
			}
		}

		return expectedMessageSize;
	}

	public Collection<StoredMessageBatch> toStoredMessageBatches () {
		Map<StoredMessageKey, StoredMessageBatch> messageBatches = new HashMap<>();

		for (StoredMessage message : getMessages()) {
			StoredMessageKey messageKey = new StoredMessageKey(message.getStreamName(), message.getDirection());

			if (!messageBatches.containsKey(messageKey)) {
				messageBatches.put(messageKey, new StoredMessageBatch());
			}

			MessageToStoreBuilder builder = new MessageToStoreBuilder()
					.content(message.getContent())
					.direction(message.getDirection())
					.streamName(message.getStreamName())
					.timestamp(message.getTimestamp())
					.index(message.getIndex());
			StoredMessageMetadata metadata = message.getMetadata();
			if (metadata != null)
				metadata.toMap().forEach(builder::metadata);

			try {
				messageBatches.get(messageKey).addMessage(builder.build());
			} catch (CradleStorageException e) {
				//TODO check if needs re-throw
				logger.error("Could not add message {}:{}:{} to batch",
						message.getStreamName(),
						message.getDirection().getLabel(),
						message.getIndex());
			}
		}

		return messageBatches.values();
	}
}
