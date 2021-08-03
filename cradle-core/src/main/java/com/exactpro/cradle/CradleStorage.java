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

package com.exactpro.cradle;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import com.exactpro.cradle.intervals.IntervalsWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Storage which holds information about all data sent or received and test events.
 */
public abstract class CradleStorage
{
	private static final Logger logger = LoggerFactory.getLogger(CradleStorage.class);
	public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;
	
	private final String book;
	private volatile boolean disposed = false;
	
	public CradleStorage(String book)
	{
		logger.info("Creating storage for book '{}'", book);
		this.book = book;
	}
	
	
	protected abstract void doDispose() throws CradleStorageException;
	
	
	protected abstract void doStoreMessageBatch(StoredMessageBatch batch) throws IOException;
	protected abstract CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch);
	
	
	protected abstract void doStoreTestEvent(StoredTestEvent event) throws IOException;
	protected abstract CompletableFuture<Void> doStoreTestEventAsync(StoredTestEvent event);
	protected abstract void doUpdateParentTestEvents(StoredTestEvent event) throws IOException;
	protected abstract CompletableFuture<Void> doUpdateParentTestEventsAsync(StoredTestEvent event);
	protected abstract void doUpdateEventStatus(StoredTestEventWrapper event, boolean success) throws IOException;
	protected abstract CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEventWrapper event, boolean success);
	
	
	protected abstract StoredMessage doGetMessage(StoredMessageId id) throws IOException;
	protected abstract CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id);
	protected abstract Collection<StoredMessage> doGetMessageBatch(StoredMessageId id) throws IOException;
	protected abstract CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id);
	
	protected abstract Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException;
	protected abstract CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter);
	protected abstract Iterable<StoredMessageBatch> doGetMessagesBatches(StoredMessageFilter filter) throws IOException;
	protected abstract CompletableFuture<Iterable<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter);
	
	protected abstract long doGetLastMessageIndex(String streamName, Direction direction) throws IOException;
	protected abstract Collection<String> doGetStreams() throws IOException;
	
	
	protected abstract StoredTestEventWrapper doGetTestEvent(StoredTestEventId id) throws IOException;
	protected abstract CompletableFuture<StoredTestEventWrapper> doGetTestEventAsync(StoredTestEventId ids);
	
	protected abstract Iterable<StoredTestEventWrapper> doGetRootTestEvents(Instant from, Instant to, Order order) 
			throws CradleStorageException, IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEventWrapper>> doGetRootTestEventsAsync(Instant from, Instant to, Order order)
			throws CradleStorageException;
	protected abstract Iterable<StoredTestEventWrapper> doGetTestEvents(StoredTestEventId parentId, Instant from, Instant to, Order order) 
			throws CradleStorageException, IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEventWrapper>> doGetTestEventsAsync(StoredTestEventId parentId, 
			Instant from, Instant to, Order order) throws CradleStorageException;
	protected abstract Iterable<StoredTestEventWrapper> doGetTestEvents(Instant from, Instant to, Order order) 
			throws CradleStorageException, IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEventWrapper>> doGetTestEventsAsync(Instant from, Instant to, Order order)
			throws CradleStorageException;
	
	
	/**
	 * IntervalsWorker is used to work with Crawler intervals
	 * @return instance of IntervalsWorker
	 */
	public abstract IntervalsWorker getIntervalsWorker();
	
	
	/**
	 * Disposes resources occupied by storage which means closing of opened connections, flushing all buffers, etc.
	 * @throws CradleStorageException if there was error during storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	public final void dispose() throws CradleStorageException
	{
		logger.info("Disposing storage for book '{}'", book);
		disposed = true;
		doDispose();
		logger.info("Storage disposed");
	}
	
	/**
	 * @return true if storage is already disposed and false if it is not disposed, including the case when disposal failed with error
	 */
	public final boolean isDisposed()
	{
		return disposed;
	}
	
	
	/**
	 * @return name of book the storage is bound to
	 */
	public String getBookName()
	{
		return book;
	}
	
	
	/**
	 * Writes data about given message batch to storage.
	 * @param batch data to write
	 * @throws IOException if data writing failed
	 */
	public final void storeMessageBatch(StoredMessageBatch batch) throws IOException
	{
		logger.debug("Storing message batch {}", batch.getId());
		doStoreMessageBatch(batch);
		logger.debug("Message batch {} has been stored", batch.getId());
	}
	
	
	/**
	 * Asynchronously writes data about given message batch to storage.
	 * @param batch data to write
	 * @return future to get know if storing was successful
	 */
	public final CompletableFuture<Void> storeMessageBatchAsync(StoredMessageBatch batch)
	{
		logger.debug("Storing message batch {} asynchronously", batch.getId());
		return doStoreMessageBatchAsync(batch)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while storing message batch "+batch.getId()+" asynchronously", error);
					else
						logger.debug("Message batch {} has been stored asynchronously", batch.getId());
				});
	}
	
	
	/**
	 * Writes data about given test event to storage
	 * @param event data to write
	 * @throws IOException if data writing failed
	 */
	public final void storeTestEvent(StoredTestEvent event) throws IOException
	{
		logger.debug("Storing test event {}", event.getId());
		try
		{
			TestEventUtils.validateTestEvent(event, !(event instanceof StoredTestEventBatch));
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Invalid test event", e);
		}
		doStoreTestEvent(event);
		logger.debug("Test event {} has been stored", event.getId());
		if (event.getParentId() != null)
		{
			logger.debug("Updating parents of test event {}", event.getId());
			doUpdateParentTestEvents(event);
			logger.debug("Parents of test event {} have been updated", event.getId());
		}
	}
	
	/**
	 * Asynchronously writes data about given test event to storage
	 * @param event data to write
	 * @throws IOException if data is invalid
	 * @return future to get know if storing was successful
	 */
	public final CompletableFuture<Void> storeTestEventAsync(StoredTestEvent event) throws IOException
	{
		logger.debug("Storing test event {} asynchronously", event.getId());
		try
		{
			TestEventUtils.validateTestEvent(event, !(event instanceof StoredTestEventBatch));
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Invalid test event", e);
		}
		
		CompletableFuture<Void> result1 = doStoreTestEventAsync(event)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while storing test event "+event.getId()+" asynchronously", error);
					else
						logger.debug("Test event {} has been stored asynchronously", event.getId());
				});
		
		if (event.getParentId() == null)
			return result1;
		
		logger.debug("Updating parents of test event {} asynchronously", event.getId());
		CompletableFuture<Void> result2 = doUpdateParentTestEventsAsync(event)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while updating parent of test event "+event.getId()+" asynchronously", error);
					else
						logger.debug("Parents of test event {} have been updated asynchronously", event.getId());
				});
		return CompletableFuture.allOf(result1, result2);
	}

	/**
	 * Retrieves message data stored under given ID
	 * @param id of stored message to retrieve
	 * @return data of stored message
	 * @throws IOException if message data retrieval failed
	 */
	public final StoredMessage getMessage(StoredMessageId id) throws IOException
	{
		logger.debug("Getting message {}", id);
		StoredMessage result = doGetMessage(id);
		logger.debug("Message {} got", id);
		return result;
	}
	
	/**
	 * Asynchronously retrieves message data stored under given ID
	 * @param id of stored message to retrieve
	 * @return future to obtain data of stored message
	 */
	public final CompletableFuture<StoredMessage> getMessageAsync(StoredMessageId id)
	{
		logger.debug("Getting message {} asynchronously", id);
		return doGetMessageAsync(id)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting message "+id+" asynchronously", error);
					else
						logger.debug("Message {} got asynchronously", id);
				});
	}
	
	/**
	 * Retrieves batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @return collection with messages stored in batch
	 * @throws IOException if batch data retrieval failed
	 */
	public final Collection<StoredMessage> getMessageBatch(StoredMessageId id) throws IOException
	{
		logger.debug("Getting message batch by message ID {}", id);
		Collection<StoredMessage> result = doGetMessageBatch(id);
		logger.debug("Message batch by message ID {} got", id);
		return result;
	}
	
	/**
	 * Asynchronously retrieves batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @return future to obtain collection with messages stored in batch
	 */
	public final CompletableFuture<Collection<StoredMessage>> getMessageBatchAsync(StoredMessageId id)
	{
		logger.debug("Getting message batch by message ID {} asynchronously", id);
		return doGetMessageBatchAsync(id)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting message batch by message ID "+id+" asynchronously", error);
					else
						logger.debug("Message batch by message ID {} got asynchronously", id);
				});
	}
	
	/**
	 * Retrieves last stored message index for given stream and direction. Use result of this method to continue sequence of message indices.
	 * Indices are scoped by stream and direction, so both arguments are required 
	 * @param streamName to get message index for 
	 * @param direction to get message index for
	 * @return last stored message index for given arguments, if it is present, -1 otherwise
	 * @throws IOException if index retrieval failed
	 */
	public final long getLastMessageIndex(String streamName, Direction direction) throws IOException
	{
		logger.debug("Getting last stored message index for stream '{}' and direction '{}'", streamName, direction.getLabel());
		long result = doGetLastMessageIndex(streamName, direction);
		logger.debug("Message index {} got", result);
		return result;
	}
	
	
	/**
	 * Retrieves test event data stored under given ID
	 * @param id of stored test event to retrieve
	 * @return data of stored test event
	 * @throws IOException if test event data retrieval failed
	 */
	public final StoredTestEventWrapper getTestEvent(StoredTestEventId id) throws IOException
	{
		logger.debug("Getting test event {}", id);
		StoredTestEventWrapper result = doGetTestEvent(id);
		logger.debug("Test event {} got", id);
		return result;
	}
	
	/**
	 * Asynchronously retrieves test event data stored under given ID
	 * @param id of stored test event to retrieve
	 * @return future to obtain data of stored test event
	 */
	public final CompletableFuture<StoredTestEventWrapper> getTestEventAsync(StoredTestEventId id)
	{
		logger.debug("Getting test event {} asynchronously", id);
		
		CompletableFuture<StoredTestEventWrapper> result = doGetTestEventAsync(id)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting test event "+id+" asynchronously", error);
					else
						logger.debug("Test event {} got asynchronously", id);
				});
		return result;
	}

	/**
	 * Allows to enumerate stored messages, optionally filtering them by given conditions
	 * @param filter defines conditions to filter messages by. Use null if no filtering is needed
	 * @return iterable object to enumerate messages
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredMessage> getMessages(StoredMessageFilter filter) throws IOException
	{
		logger.debug("Filtering messages by {}", filter);
		Iterable<StoredMessage> result = doGetMessages(filter);
		logger.debug("Prepared iterator for messages filtered by {}", filter);
		return result;
	}


	/**
	 * Allows to enumerate stored message batches, optionally filtering them by given conditions
	 * @param filter defines conditions to filter message batches by. Use null if no filtering is needed
	 * @return iterable object to enumerate message batches
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredMessageBatch> getMessagesBatches(StoredMessageFilter filter) throws IOException
	{
		logger.debug("Filtering message batches by {}", filter);
		Iterable<StoredMessageBatch> result = doGetMessagesBatches(filter);
		logger.debug("Prepared iterator for message batches filtered by {}", filter);
		return result;
	}

	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate stored messages, 
	 * optionally filtering them by given conditions
	 * @param filter defines conditions to filter messages by. Use null if no filtering is needed
	 * @return future to obtain iterable object to enumerate messages
	 */
	public final CompletableFuture<Iterable<StoredMessage>> getMessagesAsync(StoredMessageFilter filter)
	{
		logger.debug("Asynchronously getting messages filtered by {}", filter);
		CompletableFuture<Iterable<StoredMessage>> result = doGetMessagesAsync(filter)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting messages filtered by "+filter+" asynchronously", error);
					else
						logger.debug("Iterator for messages filtered by {} got asynchronously", filter);
				});
		return result;
	}

	/**
	 * Allows to asynchronously obtain iterable object to enumerate stored message batches,
	 * optionally filtering them by given conditions
	 * @param filter defines conditions to filter message batches by. Use null if no filtering is needed
	 * @return future to obtain iterable object to enumerate message batches
	 */
	public final CompletableFuture<Iterable<StoredMessageBatch>> getMessagesBatchesAsync(StoredMessageFilter filter)
	{
		logger.debug("Asynchronously getting message batches filtered by {}", filter);
		CompletableFuture<Iterable<StoredMessageBatch>> result = doGetMessagesBatchesAsync(filter)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting message batches filtered by "+filter+" asynchronously", error);
					else
						logger.debug("Iterator for message batches filtered by {} got asynchronously", filter);
				});

		return result;
	}

	/**
	 * Allows to enumerate root test events started in given range of timestamps in direct order. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return iterable object to enumerate root test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getRootTestEvents(Instant from, Instant to) throws CradleStorageException, IOException
	{
		return getRootTestEvents(from, to, Order.DIRECT);
	}

	/**
	 * Allows to enumerate root test events started in given range of timestamps in specified order. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @param order defines sorting order   
	 * @return iterable object to enumerate root test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getRootTestEvents(Instant from, Instant to, Order order) throws CradleStorageException, IOException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");

		logger.debug("Getting root test events from range {}..{} in {} order", from, to, order);
		Iterable<StoredTestEventWrapper> result = doGetRootTestEvents(from, to, order);
		logger.debug("Prepared iterator for root test events from range {}..{} in {} order", from, to, order);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate root test events started in given range of timestamps
	 * in direct order. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return future to obtain iterable object to enumerate root test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventWrapper>> getRootTestEventsAsync(Instant from, Instant to) throws CradleStorageException
	{
		return getRootTestEventsAsync(from, to, Order.DIRECT);
	}


	/**
	 * Allows to asynchronously obtain iterable object to enumerate root test events started in given range of timestamps
	 * in specified order. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @param order defines sorting order   
	 * @return future to obtain iterable object to enumerate root test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventWrapper>> getRootTestEventsAsync(Instant from, Instant to, Order order) throws CradleStorageException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");

		logger.debug("Getting root test events from range {}..{} in {} order asynchronously", from, to, order);

		CompletableFuture<Iterable<StoredTestEventWrapper>> result = doGetRootTestEventsAsync(from, to, order)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting root test events from range "+from+".."+to+" asynchronously", error);
					else
						logger.debug("Iterator for root test events from range {}..{} in {} order got asynchronously", from, to, order);
				});
		return result;
	}


	/**
	 * Allows to enumerate children of test event with given ID that started in given range of timestamps ordered by ascending.
	 * Both boundaries (from and to) should be specified
	 * @param parentId ID of parent test event
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getTestEvents(StoredTestEventId parentId, Instant from, Instant to) 
			throws CradleStorageException, IOException
	{
		return getTestEvents(parentId, from, to, Order.DIRECT);
	}


	/**
	 * Allows to enumerate children of test event with given ID that started in given range of timestamps in specified order.
	 * Both boundaries (from and to) should be specified
	 * @param parentId ID of parent test event
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @param order defines sorting order
	 * @return iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getTestEvents(StoredTestEventId parentId, Instant from, Instant to, Order order)
			throws CradleStorageException, IOException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");

		logger.debug("Getting child test events of {} from range {}..{} in {} order", parentId, from, to, order);
		Iterable<StoredTestEventWrapper> result = doGetTestEvents(parentId, from, to, order);
		logger.debug("Prepared iterator for child test events of {} from range {}..{} in {} order", parentId, from, to, order);
		return result;
	}

	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate children of test event with given ID 
	 * that started in given range of timestamps in ascending order.
	 * Both boundaries (from and to) should be specified
	 * @param parentId ID of parent test event
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return future to obtain iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventWrapper>> getTestEventsAsync(StoredTestEventId parentId, 
			Instant from, Instant to) throws CradleStorageException
	{
		return getTestEventsAsync(parentId, from, to, Order.DIRECT);
	}

	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate children of test event with given ID 
	 * that started in given range of timestamps in specified order.
	 * Both boundaries (from and to) should be specified
	 * @param parentId ID of parent test event
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @param order defines sorting order
	 * @return future to obtain iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventWrapper>> getTestEventsAsync(StoredTestEventId parentId,
			Instant from, Instant to, Order order) throws CradleStorageException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");

		logger.debug("Getting child test events of {} from range {}..{} in {} order asynchronously", parentId, from, to, order);

		CompletableFuture<Iterable<StoredTestEventWrapper>> result = doGetTestEventsAsync(parentId, from, to, order)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting child test events of "+parentId+" from range "+from+".."+to+" asynchronously", error);
					else
						logger.debug("Iterator for child test events of {} from range {}..{} in {} order got asynchronously", parentId, from, to, order);
				});
		return result;
	}
	
	
	/**
	 * Allows to enumerate test events started in given range of timestamps in ascending order. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getTestEvents(Instant from, Instant to) throws CradleStorageException, IOException
	{
		return getTestEvents(from, to, Order.DIRECT);
	}

	/**
	 * Allows to enumerate test events started in given range of timestamps in specified order. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @param order defines sorting order
	 * @return iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getTestEvents(Instant from, Instant to, Order order) throws CradleStorageException, IOException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");

		logger.debug("Getting test events from range {}..{} in {} order", from, to, order);
		Iterable<StoredTestEventWrapper> result = doGetTestEvents(from, to, order);
		logger.debug("Prepared iterator for test events from range {}..{} in {} order", from, to, order);
		return result;
	}


	/**
	 * Allows to asynchronously obtain iterable object to enumerate test events started in given range of timestamps. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return future to obtain iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventWrapper>> getTestEventsAsync(Instant from, Instant to) throws CradleStorageException
	{
		return getTestEventsAsync(from, to, Order.DIRECT);
	}

	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate test events started in given range of timestamps. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return future to obtain iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventWrapper>> getTestEventsAsync(Instant from, Instant to, Order order) throws CradleStorageException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");

		logger.debug("Getting test events from range {}..{} in {} order asynchronously", from, to, order);

		CompletableFuture<Iterable<StoredTestEventWrapper>> result = doGetTestEventsAsync(from, to, order)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while getting test events from range "+from+".."+to+" asynchronously", error);
					else
						logger.debug("Iterator for test events from range {}..{} in {} order got asynchronously", from, to, order);
				});
		return result;
	}
	
	
	/**
	 * Obtains collection of streams whose messages are currently saved in storage
	 * @return collection of stream names
	 * @throws IOException if data retrieval failed
	 */
	public final Collection<String> getStreams() throws IOException
	{
		logger.debug("Getting list of streams");
		Collection<String> result = doGetStreams();
		logger.debug("List of streams got");
		return result;
	}
	
	
	public final void updateEventStatus(StoredTestEventWrapper event, boolean success) throws IOException
	{
		logger.debug("Updating status of event {}", event.getId());
		doUpdateEventStatus(event, success);
		logger.debug("Status of event {} has been updated", event.getId());
	}

	public final CompletableFuture<Void> updateEventStatusAsync(StoredTestEventWrapper event, boolean success)
	{
		logger.debug("Asynchronously updating status of event {}", event.getId());
		CompletableFuture<Void> result = doUpdateEventStatusAsync(event, success)
				.whenComplete((r, error) -> {
					if (error != null)
						logger.error("Error while asynchronously updating status of event "+event.getId());
					else
						logger.debug("Status of event {} updated asynchronously", event.getId());
				});
		return result;
	}
}
