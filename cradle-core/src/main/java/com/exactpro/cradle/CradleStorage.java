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
import com.exactpro.cradle.testevents.StoredTestEventMetadata;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Storage which holds information about all data sent or verified and generated reports.
 */
public abstract class CradleStorage
{
	private static final Logger logger = LoggerFactory.getLogger(CradleStorage.class);
	
	private String instanceId;
	
	private volatile boolean workingState = false;
	
	/**
	 * Initializes internal objects of storage, i.e. creates needed connections and facilities and obtains ID of data instance with given name.
	 * If data instance with that name doesn't exist, makes in storage the new record with given name, returning ID of that record
	 * @param instanceName name of data instance. Will be used to mark written data
	 * @param prepareStorage flag that indicates if underlying storage on disk can be created or its structure can be updated, if needed
	 * @return ID of data instance as recorded in storage
	 * @throws CradleStorageException if storage initialization failed
	 */
	protected abstract String doInit(String instanceName, boolean prepareStorage) throws CradleStorageException;
	protected abstract void doDispose() throws CradleStorageException;
	
	
	protected abstract void doStoreMessageBatch(StoredMessageBatch batch) throws IOException;
	protected abstract CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch);
	protected abstract void doStoreProcessedMessageBatch(StoredMessageBatch batch) throws IOException;
	protected abstract CompletableFuture<Void> doStoreProcessedMessageBatchAsync(StoredMessageBatch batch);
	protected abstract void doStoreTestEvent(StoredTestEvent event) throws IOException;
	protected abstract CompletableFuture<Void> doStoreTestEventAsync(StoredTestEvent event);
	protected abstract void doStoreTestEventMessagesLink(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messageIds) throws IOException;
	protected abstract CompletableFuture<Void> doStoreTestEventMessagesLinkAsync(StoredTestEventId eventId, StoredTestEventId batchId, 
			Collection<StoredMessageId> messageIds);
	protected abstract StoredMessage doGetMessage(StoredMessageId id) throws IOException;
	protected abstract CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id);
	protected abstract Collection<StoredMessage> doGetMessageBatch(StoredMessageId id) throws IOException;
	protected abstract CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id);
	protected abstract StoredMessage doGetProcessedMessage(StoredMessageId id) throws IOException;
	protected abstract CompletableFuture<StoredMessage> doGetProcessedMessageAsync(StoredMessageId id);
	protected abstract long doGetLastMessageIndex(String streamName, Direction direction) throws IOException;
	protected abstract long doGetLastProcessedMessageIndex(String streamName, Direction direction) throws IOException;
	protected abstract StoredMessageId doGetNearestMessageId(String streamName, Direction direction, Instant timestamp, TimeRelation timeRelation) throws IOException;
	protected abstract CompletableFuture<StoredMessageId> doGetNearestMessageIdAsync(String streamName, Direction direction, Instant timestamp, TimeRelation timeRelation);
	protected abstract StoredTestEventWrapper doGetTestEvent(StoredTestEventId id) throws IOException;
	protected abstract CompletableFuture<StoredTestEventWrapper> doGetTestEventAsync(StoredTestEventId ids);
	protected abstract Iterable<StoredTestEventWrapper> doGetCompleteTestEvents(Set<StoredTestEventId> ids) throws IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEventWrapper>> doGetCompleteTestEventsAsync(Set<StoredTestEventId> ids);
	protected abstract Collection<String> doGetStreams() throws IOException;
	protected abstract Collection<Instant> doGetRootTestEventsDates() throws IOException;
	protected abstract Collection<Instant> doGetTestEventsDates(StoredTestEventId parentId) throws IOException;
	protected abstract void doUpdateEventStatus(StoredTestEventWrapper event, boolean success) throws IOException;
	protected abstract CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEventWrapper event, boolean success);

	public abstract CradleObjectsFactory getObjectsFactory();
	
	/**
	 * @return number of submitted and currently executed asynchronous requests
	 */
	public abstract int getActiveAsyncRequests();
	
	/**
	 * @return number of submitted asynchronous requests that are not started yet
	 */
	public abstract int getPendingAsyncRequests();
	
	
	/**
	 * TestEventsMessagesLinker is used to obtain links between test events and messages
	 * @return instance of TestEventsMessagesLinker
	 */
	public abstract TestEventsMessagesLinker getTestEventsMessagesLinker();

	/**
	 * IntervalsWorker is used to work with Crawler intervals
	 * @return instance of IntervalsWorker
	 */
	public abstract IntervalsWorker getIntervalsWorker();
	
	protected abstract Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException;
	protected abstract CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter);
	protected abstract Iterable<StoredMessageBatch> doGetMessagesBatches(StoredMessageFilter filter) throws IOException;
	protected abstract CompletableFuture<Iterable<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter);
	protected abstract Iterable<StoredTestEventMetadata> doGetRootTestEvents(Instant from, Instant to) 
			throws CradleStorageException, IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEventMetadata>> doGetRootTestEventsAsync(Instant from, Instant to) 
			throws CradleStorageException;
	protected abstract Iterable<StoredTestEventMetadata> doGetTestEvents(StoredTestEventId parentId, Instant from, Instant to) 
			throws CradleStorageException, IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsAsync(StoredTestEventId parentId, Instant from, Instant to) 
			throws CradleStorageException;
	protected abstract Iterable<StoredTestEventMetadata> doGetTestEvents(Instant from, Instant to) throws CradleStorageException, IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEventMetadata>> doGetTestEventsAsync(Instant from, Instant to) throws CradleStorageException;
	
	
	/**
	 * Initializes storage, i.e. creates needed connections and gets ready to write data marked with given instance name. 
	 * Storage on disk is created/updated if this is allowed with prepareStorage flag
	 * @param instanceName name of current data instance. Will be used to mark written data
	 * @param prepareStorage flag that indicates if underlying storage on disk can be created or its structure can be updated, if needed
	 * @throws CradleStorageException if storage initialization failed
	 */
	public final void init(String instanceName, boolean prepareStorage) throws CradleStorageException
	{
		if (workingState)
			throw new CradleStorageException("Already initialized");
		
		logger.info("Storage initialization started");
		instanceId = doInit(instanceName, prepareStorage);
	}
	
	/**
	 * Switches storage from its initial state to working state. This affects storage operations.
	 */
	public final void initFinish()
	{
		workingState = true;
		logger.info("Storage initialization finished");
	}
	
	/**
	 * @return ID of current application instance as recorded in storage
	 */
	public String getInstanceId()
	{
		return instanceId;
	}
	
	
	/**
	 * Disposes resources occupied by storage which means closing of opened connections, flushing all buffers, etc.
	 * @throws CradleStorageException if there was error during storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	public final void dispose() throws CradleStorageException
	{
		doDispose();
		logger.info("Storage disposed");
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
		CompletableFuture<Void> result = doStoreMessageBatchAsync(batch);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while storing message batch "+batch.getId()+" asynchronously", error);
				else
					logger.debug("Message batch {} has been stored asynchronously", batch.getId());
			});
		return result;
	}
	
	/**
	 * Writes data about given processed message batch to storage.
	 * @param batch data to write
	 * @throws IOException if data writing failed
	 */
	public final void storeProcessedMessageBatch(StoredMessageBatch batch) throws IOException
	{
		logger.debug("Storing processed message batch {}", batch.getId());
		doStoreProcessedMessageBatch(batch);
		logger.debug("Processed message batch {} has been stored", batch.getId());
	}
	
	/**
	 * Asynchronously writes data about given processed message batch to storage.
	 * @param batch data to write
	 * @return future to get know if storing was successful
	 */
	public final CompletableFuture<Void> storeProcessedMessageBatchAsync(StoredMessageBatch batch)
	{
		logger.debug("Storing processed message batch {} asynchronously", batch.getId());
		CompletableFuture<Void> result = doStoreProcessedMessageBatchAsync(batch);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while storing processed message batch "+batch.getId()+" asynchronously", error);
				else
					logger.debug("Processed message batch {} has been stored asynchronously", batch.getId());
			});
		return result;
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
		
		CompletableFuture<Void> result = doStoreTestEventAsync(event);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while storing test event "+event.getId()+" asynchronously", error);
				else
					logger.debug("Test event {} has been stored asynchronously", event.getId());
			});
		return result;
	}

	/**
	 * Writes to storage the links between given test event and messages
	 * @param eventId ID of stored test event
	 * @param batchId ID of batch where event is stored, if applicable
	 * @param messagesIds collection of stored message IDs
	 * @throws IOException if data writing failed
	 */
	public final void storeTestEventMessagesLink(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messagesIds) throws IOException
	{
		logger.debug("Storing links between test event {} and {} message(s)", eventId, messagesIds.size());
		doStoreTestEventMessagesLink(eventId, batchId, messagesIds);
		logger.debug("Links between test event {} and {} message(s) have been stored", eventId, messagesIds.size());
	}

	/**
	 * Asynchronously writes to storage the links between given test event and messages
	 * @param eventId ID of stored test event
	 * @param batchId ID of batch where event is stored, if applicable
	 * @param messagesIds collection of stored message IDs
	 * @return future to get know if storing was successful
	 */
	public final CompletableFuture<Void> storeTestEventMessagesLinkAsync(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messagesIds)
	{
		logger.debug("Storing links between test event {} and {} message(s) asynchronously", eventId, messagesIds.size());
		CompletableFuture<Void> result = doStoreTestEventMessagesLinkAsync(eventId, batchId, messagesIds);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while storing links between test event "+eventId+" and "+messagesIds.size()+" message(s) asynchronously", error);
				else
					logger.debug("Links between test event {} and {} message(s) have been stored asynchronously", eventId, messagesIds.size());
			});
		return result;
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
		CompletableFuture<StoredMessage> result = doGetMessageAsync(id);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting message "+id+" asynchronously", error);
				else
					logger.debug("Message {} got asynchronously", id);
			});
		return result;
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
		CompletableFuture<Collection<StoredMessage>> result = doGetMessageBatchAsync(id);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting message batch by message ID "+id+" asynchronously", error);
				else
					logger.debug("Message batch by message ID {} got asynchronously", id);
			});
		return result;
	}
	
	/**
	 * Retrieves processed message data stored under given ID
	 * @param id of stored processed message to retrieve
	 * @return data of stored processed message
	 * @throws IOException if message data retrieval failed
	 */
	public final StoredMessage getProcessedMessage(StoredMessageId id) throws IOException
	{
		logger.debug("Getting processed message {}", id);
		StoredMessage result = doGetProcessedMessage(id);
		logger.debug("Processed message {} got", id);
		return result;
	}
	
	/**
	 * Asynchronously retrieves processed message data stored under given ID
	 * @param id of stored processed message to retrieve
	 * @return future to obtain data of stored processed message
	 */
	public final CompletableFuture<StoredMessage> getProcessedMessageAsync(StoredMessageId id)
	{
		logger.debug("Getting processed message {} asynchronously", id);
		CompletableFuture<StoredMessage> result = doGetProcessedMessageAsync(id);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting processed message "+id+" asynchronously", error);
				else
					logger.debug("Processed message {} got asynchronously", id);
			});
		return result;
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
	 * Retrieves last processed message index for given stream and direction. Use result of this method to continue sequence of message indices.
	 * Indices are scoped by stream and direction, so both arguments are required 
	 * @param streamName to get message index for 
	 * @param direction to get message index for
	 * @return last processed message index for given arguments, if it is present, -1 otherwise
	 * @throws IOException if index retrieval failed
	 */
	public final long getLastProcessedMessageIndex(String streamName, Direction direction) throws IOException
	{
		logger.debug("Getting last processed message index for stream '{}' and direction '{}'", streamName, direction.getLabel());
		long result = doGetLastProcessedMessageIndex(streamName, direction);
		logger.debug("Processed message index {} got", result);
		return result;
	}
	
	/**
	 * Retrieves ID of first message appeared in given timestamp or before/after it
	 * @param streamName to which the message should be related
	 * @param direction of message
	 * @param timestamp to search for messages
	 * @param timeRelation defines if need to find message appeared before given timestamp or after it
	 * @return ID of first message appeared in given timestamp or before/after it
	 * @throws IOException if data retrieval failed
	 */
	public final StoredMessageId getNearestMessageId(String streamName, Direction direction, Instant timestamp, TimeRelation timeRelation) throws IOException
	{
		logger.debug("Getting ID of first message appeared on {} or {} for stream '{}' and direction '{}'", 
				timestamp, timeRelation.getLabel(), streamName, direction.getLabel());
		StoredMessageId result = doGetNearestMessageId(streamName, direction, timestamp, timeRelation);
		logger.debug("First message ID appeared on {} or {} for stream '{}' and direction '{}' got", 
				timestamp, timeRelation.getLabel(), streamName, direction.getLabel());
		return result;
	}

	/**
	 * Asynchronously retrieves ID of first message appeared in given timestamp or before/after it
	 * @param streamName to which the message should be related
	 * @param direction of message
	 * @param timestamp to search for messages
	 * @param timeRelation defines if need to find message appeared before given timestamp or after it
	 * @return ID of first message appeared in given timestamp or before/after it
	 * @throws IOException if data retrieval failed
	 */
	public final CompletableFuture<StoredMessageId> getNearestMessageIdAsync(String streamName, Direction direction, Instant timestamp, TimeRelation timeRelation) throws IOException
	{
		logger.debug(
				"Asynchronously getting ID of first message appeared on {} or {} for stream '{}' and direction '{}'",
				timestamp, timeRelation.getLabel(), streamName, direction.getLabel());
		CompletableFuture<StoredMessageId> result =	doGetNearestMessageIdAsync(streamName, direction, timestamp, timeRelation);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error(
							"Error while getting first message ID appeared on {} or {} for stream '{}' and" +
									" direction '{}'",
							timestamp, timeRelation.getLabel(), streamName, direction.getLabel());
				else
					logger.debug(
							"First message ID appeared on {} or {} for stream '{}' and direction '{}' got",
							timestamp, timeRelation.getLabel(), streamName, direction.getLabel());
			});
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
		
		CompletableFuture<StoredTestEventWrapper> result = doGetTestEventAsync(id);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting test event "+id+" asynchronously", error);
				else
					logger.debug("Test event {} got asynchronously", id);
			});
		return result;
	}

	/**
	 * Retrieves test events data stored under given IDs
	 * @param ids set of stored test event to retrieve
	 * @return data of stored test event
	 * @throws IOException if test event data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getCompleteTestEvents(Set<StoredTestEventId> ids) throws IOException
	{
		logger.debug("Getting test events {}", ids);
		Iterable<StoredTestEventWrapper> result = doGetCompleteTestEvents(ids);
		logger.debug("Test events {} got", ids);
		return result;
	}

	/**
	 * Asynchronously retrieves test events data stored under given IDs
	 * @param ids set of stored test event to retrieve
	 * @return future to obtain data of stored test event
	 */
	public final CompletableFuture<Iterable<StoredTestEventWrapper>> getCompleteTestEventsAsync(Set<StoredTestEventId> ids)
	{
		logger.debug("Getting test events {} asynchronously", ids);

		CompletableFuture<Iterable<StoredTestEventWrapper>> result = doGetCompleteTestEventsAsync(ids);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting test events "+ids+" asynchronously", error);
				else
					logger.debug("Test events {} got asynchronously", ids);
			});
		return result;
	}
	
	/**
	 * Allows to enumerate stored messages, optionally filtering them by given conditions
	 * @param filter defines conditions to filter messages by. Use null is no filtering is needed
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
	 * @param filter defines conditions to filter message batches by. Use null is no filtering is needed
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
	 * Allows to asynchronously obtain iterable object to enumerate stored messages, optionally filtering them by given conditions
	 * @param filter defines conditions to filter messages by. Use null is no filtering is needed
	 * @return future to obtain iterable object to enumerate messages
	 */
	public final CompletableFuture<Iterable<StoredMessage>> getMessagesAsync(StoredMessageFilter filter)
	{
		logger.debug("Asynchronously getting messages filtered by {}", filter);
		CompletableFuture<Iterable<StoredMessage>> result = doGetMessagesAsync(filter);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting messages filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Iterator for messages filtered by {} got asynchronously", filter);
			});
		return result;
	}

	/**
	 * Allows to asynchronously obtain iterable object to enumerate stored message batches, optionally filtering them by given conditions
	 * @param filter defines conditions to filter message batches by. Use null is no filtering is needed
	 * @return future to obtain iterable object to enumerate message batches
	 */
	public final CompletableFuture<Iterable<StoredMessageBatch>> getMessagesBatchesAsync(StoredMessageFilter filter)
	{
		logger.debug("Asynchronously getting message batches filtered by {}", filter);
		CompletableFuture<Iterable<StoredMessageBatch>> result = doGetMessagesBatchesAsync(filter);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting message batches filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Iterator for message batches filtered by {} got asynchronously", filter);
			});
		return result;
	}
	
	/**
	 * Allows to enumerate root test events started in given range of timestamps. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return iterable object to enumerate root test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventMetadata> getRootTestEvents(Instant from, Instant to) throws CradleStorageException, IOException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");
		
		logger.debug("Getting root test events from range {}..{}", from, to);
		Iterable<StoredTestEventMetadata> result = doGetRootTestEvents(from, to);
		logger.debug("Prepared iterator for root test events from range {}..{}", from, to);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate root test events started in given range of timestamps. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return future to obtain iterable object to enumerate root test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventMetadata>> getRootTestEventsAsync(Instant from, Instant to) throws CradleStorageException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");
		
		logger.debug("Getting root test events from range {}..{} asynchronously", from, to);
		
		CompletableFuture<Iterable<StoredTestEventMetadata>> result = doGetRootTestEventsAsync(from, to);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting root test events from range "+from+".."+to+" asynchronously", error);
				else
					logger.debug("Iterator for root test events from range {}..{} got asynchronously", from, to);
			});
		return result;
	}
	
	
	/**
	 * Allows to enumerate children of test event with given ID that started in given range of timestamps.
	 * Both boundaries (from and to) should be specified
	 * @param parentId ID of parent test event
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventMetadata> getTestEvents(StoredTestEventId parentId, Instant from, Instant to) 
			throws CradleStorageException, IOException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");
		
		logger.debug("Getting child test events of {} from range {}..{}", parentId, from, to);
		Iterable<StoredTestEventMetadata> result = doGetTestEvents(parentId, from, to);
		logger.debug("Prepared iterator for child test events of {} from range {}..{}", parentId, from, to);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate children of test event with given ID that started in given range of timestamps.
	 * Both boundaries (from and to) should be specified
	 * @param parentId ID of parent test event
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return future to obtain iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEventMetadata>> getTestEventsAsync(StoredTestEventId parentId, 
			Instant from, Instant to) throws CradleStorageException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");
		
		logger.debug("Getting child test events of {} from range {}..{} asynchronously", parentId, from, to);
		
		CompletableFuture<Iterable<StoredTestEventMetadata>> result = doGetTestEventsAsync(parentId, from, to);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting child test events of "+parentId+" from range "+from+".."+to+" asynchronously", error);
				else
					logger.debug("Iterator for child test events of {} from range {}..{} got asynchronously", parentId, from, to);
			});
		return result;
	}
	
	
	/**
	 * Allows to enumerate test events started in given range of timestamps. 
	 * Both boundaries (from and to) should be specified
	 * @param from left boundary of timestamps range
	 * @param to right boundary of timestamps range
	 * @return iterable object to enumerate test events
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventMetadata> getTestEvents(Instant from, Instant to) throws CradleStorageException, IOException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");
		
		logger.debug("Getting test events from range {}..{}", from, to);
		Iterable<StoredTestEventMetadata> result = doGetTestEvents(from, to);
		logger.debug("Prepared iterator for test events from range {}..{}", from, to);
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
	public final CompletableFuture<Iterable<StoredTestEventMetadata>> getTestEventsAsync(Instant from, Instant to) throws CradleStorageException
	{
		if (from == null || to == null)
			throw new CradleStorageException("Both boundaries (from and to) should be specified");
		
		logger.debug("Getting test events from range {}..{} asynchronously", from, to);
		
		CompletableFuture<Iterable<StoredTestEventMetadata>> result = doGetTestEventsAsync(from, to);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting test events from range "+from+".."+to+" asynchronously", error);
				else
					logger.debug("Iterator for test events from range {}..{} got asynchronously", from, to);
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
	
	/**
	 * Obtains collection of dates when root test events started
	 * @return collection of start dates
	 * @throws IOException if data retrieval failed
	 */
	public final Collection<Instant> getRootTestEventsDates() throws IOException
	{
		logger.debug("Getting list of dates of root test events");
		Collection<Instant> result = doGetRootTestEventsDates();
		logger.debug("List of dates of root test events got");
		return result;
	}
	
	/**
	 * Obtains collection of dates when children of given test event started
	 * @param parentId ID of parent test event
	 * @return collection of start dates
	 * @throws IOException if data retrieval failed
	 */
	public final Collection<Instant> getTestEventsDates(StoredTestEventId parentId) throws IOException
	{
		logger.debug("Getting list of dates of test event '{}' children", parentId);
		Collection<Instant> result = doGetTestEventsDates(parentId);
		logger.debug("List of dates of test event '{}' children got", parentId);
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
		CompletableFuture<Void> result = doUpdateEventStatusAsync(event, success);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while asynchronously updating status of event "+event.getId());
				else
					logger.debug("Status of event {} updated asynchronously", event.getId());
			});
		return result;
	}
	
	/**
	 * @return number of submitted asynchronous requests being processed, both active and pending
	 */
	public int getAsyncRequests()
	{
		return getActiveAsyncRequests()+getPendingAsyncRequests();
	}
}
