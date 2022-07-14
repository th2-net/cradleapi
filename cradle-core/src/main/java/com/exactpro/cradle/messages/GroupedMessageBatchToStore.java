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

package com.exactpro.cradle.messages;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GroupedMessageBatchToStore extends StoredGroupedMessageBatch {
	private static final Logger logger = LoggerFactory.getLogger(GroupedMessageBatchToStore.class);

	private final int maxBatchSize;
	private LocalDate batchDate;
	private final Map<SessionKey, StoredMessage> firstMessages;
	private final Map<SessionKey, StoredMessage> lastMessages;

	public GroupedMessageBatchToStore(String group, int maxBatchSize)	{
		super(group);
		this.maxBatchSize = maxBatchSize;
		this.firstMessages = new HashMap<>();
		this.lastMessages = new HashMap<>();
	}
	
	/**
	 * Indicates if the batch cannot hold more messages
	 * @return true if batch capacity is reached and the batch must be flushed to Cradle
	 */
	public boolean isFull()	{
		return batchSize >= maxBatchSize;
	}
	
	/**
	 * Shows how many bytes the batch can hold till its capacity is reached
	 * @return number of bytes the batch can hold
	 */
	public int getSpaceLeft() {
		int result = maxBatchSize - batchSize;
		return Math.max(result, 0);
	}
	
	/**
	 * Shows if batch has enough space to hold given message
	 * @param message to check against batch capacity
	 * @return true if batch has enough space to hold given message
	 */
	public boolean hasSpace(MessageToStore message)	{
		return hasSpace(MessagesSizeCalculator.calculateMessageSize(message));
	}

	private boolean hasSpace(int messageSize) {
		return batchSize + messageSize <= maxBatchSize;
	}

	/**
	 * Adds message to the batch. Batch will add correct message ID by itself, verifying message to match batch conditions.
	 * Result of this method should be used for all further operations on the message
	 * @param message to add to the batch
	 * @return immutable message object with assigned ID
	 * @throws CradleStorageException if message cannot be added to the batch due to verification failure
	 */
	public StoredMessage addMessage(MessageToStore message) throws CradleStorageException {
		int expMsgSize = MessagesSizeCalculator.calculateMessageSizeInBatch(message);
		if (!hasSpace(expMsgSize))
			throw new CradleStorageException("Batch has not enough space to hold given message");
		
		// Checking that the timestamp of a message is not from the future
		// Other checks have already been done when the MessageToStore was created
		SessionKey sessionKey = new SessionKey(message.getSessionAlias(), message.getDirection());
		Instant now = Instant.now();
		if (message.getTimestamp().isAfter(now))
			throw new CradleStorageException(
					"Message timestamp (" + TimeUtils.toLocalTimestamp(message.getTimestamp()) +
							") is greater than current timestamp (" + TimeUtils.toLocalTimestamp(now) + ")");

		long messageSeq = 0;
		if (bookId == null) {
			bookId = message.getBookId();
			if (bookId == null)
				throw new CradleStorageException("BookId for the message not set (" + message.getId() + ")");
			long i = message.getSequence();
			if (i < 0)
				throw new CradleStorageException("Sequence number for first message in batch cannot be negative");

			batchDate = TimeUtils.toLocalTimestamp(message.getTimestamp()).toLocalDate();
			messageSeq = i;
		} else {
			if (!bookId.equals(message.getBookId()))
				throw new CradleStorageException("Batch contains messages of book '" + bookId + "', "
						+ "but in your message it is '"+message.getBookId()+"'");
			LocalDate messageDate = TimeUtils.toLocalTimestamp(message.getTimestamp()).toLocalDate();
			if (!batchDate.equals(messageDate))
				throw new CradleStorageException("Batch contains messages with date '" + batchDate + "', "
						+ "but in your message it is '" + messageDate);

			StoredMessage lastMessage = lastMessages.get(sessionKey);
			if (lastMessage != null) {
				if (lastMessage.getTimestamp().isAfter(message.getTimestamp()))
					throw new CradleStorageException(String.format(
							"Message timestamp should not be before %s, but in your message it is %s",
							lastMessage.getTimestamp(),
							message.getTimestamp()
					));

				if (message.getSequence() > 0) { //I.e. message sequence is set

					messageSeq = message.getSequence();
					if (messageSeq <= lastMessage.getSequence())
						throw new CradleStorageException(String.format(
								"Sequence number should be greater than %d for the batch to contain sequenced messages, but in your message it is %d",
								lastMessage.getSequence(),
								messageSeq
						));
					if (messageSeq != lastMessage.getSequence() + 1)
						logger.warn(String.format(
								"Expected sequence number %d for the batch to contain strictly sequenced messages, but in your message it is %d",
								lastMessage.getSequence() + 1,
								messageSeq));
				} else
					messageSeq = lastMessage.getSequence() + 1;
			}
		}

		StoredMessageId msgId = new StoredMessageId(message.getBookId(), message.getSessionAlias(), message.getDirection(), message.getTimestamp(), messageSeq);
		StoredMessage msg = new StoredMessage(message, msgId, null);
		messages.add(msg);
		firstMessages.putIfAbsent(sessionKey, msg);
		lastMessages.put(sessionKey, msg);
		batchSize += expMsgSize;

		return msg;
	}

  /**
   *
   * @param batch the batch to add to the current one.
   *              The batch to add must contain message with same group name as the current one.
   *              The index of the first message in the [batch] should be greater
   *              than the last message index in the current batch.
   * @return true if the result batch meets the restriction for message count and batch size
   * @throws CradleStorageException if the batch doesn't meet the requirements regarding inner content
   */
	public boolean addBatch(GroupedMessageBatchToStore batch) throws CradleStorageException {

		if (!this.getGroup().equals(batch.getGroup()))
			throw new CradleStorageException(String.format("Batch groups differ. Current Group is %s, other Group is %s", getGroup(), batch.getGroup()));
		if (batch.isEmpty())
			return true;

		if (isEmpty()) {
			this.bookId = batch.getBookId();
			this.batchSize = batch.getBatchSize();
			messages.addAll(batch.getMessages());
			firstMessages.putAll(batch.firstMessages);
			lastMessages.putAll(batch.lastMessages);
			return true;
		}

		if (isFull() || batch.isFull())
			return false;

		int resultSize = batchSize + batch.messages.stream().mapToInt(MessagesSizeCalculator::calculateMessageSizeInBatch).sum();

		if (resultSize > maxBatchSize) {
			// cannot add because of size limit
			return false;
		}
		verifyBatch(batch);
		batch.getMessages().forEach(message -> {
			messages.add(message);
			SessionKey sessionKey = new SessionKey(message.getSessionAlias(), message.getDirection());
			lastMessages.put(sessionKey, message);
			firstMessages.putIfAbsent(sessionKey, message);
		});
		this.batchSize = resultSize;
		return true;
	}

	public Collection<MessageBatchToStore> getSessionMessageBatches() throws CradleStorageException{
		Map<SessionKey, MessageBatchToStore> batches = new HashMap<>();
		for (StoredMessage message: getMessages()) {
			SessionKey key = new SessionKey(message.getSessionAlias(), message.getDirection());
			MessageBatchToStore batch = batches.computeIfAbsent(key, k -> new MessageBatchToStore(maxBatchSize));

			StoredMessageId msgId = new StoredMessageId(message.getBookId(),
					message.getSessionAlias(),
					message.getDirection(),
					message.getTimestamp(),
					message.getSequence());

			MessageToStore batchedMessage = MessageToStore.builder()
					.id(msgId)
					.protocol(message.getProtocol())
					.content(message.getContent())
					.build();

			batch.addMessage(batchedMessage);
		}

		return batches.values();
	}


	private void verifyBatch(GroupedMessageBatchToStore otherBatch) throws CradleStorageException {
		if (!Objects.equals(bookId, otherBatch.getBookId()))
			throw new CradleStorageException(String.format("Batch BookId-s differ. Current BookId is %s, other BookId is %s", bookId, otherBatch.getBookId()));

		if (this.getFirstTimestamp().isAfter(otherBatch.getFirstTimestamp()) ||
			this.getLastTimestamp().isAfter(otherBatch.getFirstTimestamp()))
			throw new CradleStorageException(
					String.format("Batches intersect by time. Current batch %s - %s, other batch %s - %s",
							this.getFirstTimestamp(),
							this.getLastTimestamp(),
							otherBatch.getFirstTimestamp(),
							otherBatch.getLastTimestamp()));

		for (SessionKey sessionKey : lastMessages.keySet()) {
			StoredMessage otherFirstMessage = otherBatch.firstMessages.get(sessionKey);
			if (otherFirstMessage == null)
				continue;
			StoredMessage thisLastMessage = this.lastMessages.get(sessionKey);

			if (thisLastMessage.getTimestamp().isAfter(otherFirstMessage.getTimestamp()))
				throw new CradleStorageException(String.format("Batches are not ordered. Current last timestamp: %s; Other first timestamp: %s",
						thisLastMessage.getTimestamp(), otherFirstMessage.getTimestamp()));

			if (thisLastMessage.getSequence() >= otherFirstMessage.getSequence()) {
				throw new CradleStorageException(String.format("Batches are not ordered. Current last sequence number: %d; Other first sequence number: %d",
						thisLastMessage.getSequence(), otherFirstMessage.getSequence()));
			}
		}
	}

	private static class SessionKey {
		final String sessionAlias;
		final Direction direction;
		SessionKey(String sessionAlias, Direction direction) {
			this.sessionAlias = sessionAlias;
			this.direction = direction;
		}

		@Override
		public boolean equals(Object o) {
			if (! (o instanceof SessionKey))
				return false;

			SessionKey that = (SessionKey) o;
			if (!sessionAlias.equals(that.sessionAlias))
				return false;
			return direction == that.direction;
		}

		@Override
		public int hashCode() {
			int result = sessionAlias.hashCode();
			result = 31 * result + direction.hashCode();
			return result;
		}
	}
}