/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;
import com.exactpro.cradle.utils.TimeUtils;

/**
 * Storage which holds information about all data sent or verified and generated reports.
 */
public abstract class CradleStorage
{
	private static final Logger logger = LoggerFactory.getLogger(CradleStorage.class);
	
	private String instanceId;
	
	private volatile boolean workingState = false;
	
	/**
	 * Initializes internal objects of storage, i.e. creates needed connections, tables and obtains ID of application instance with given name.
	 * If no ID of instance with that name is stored, makes new record in storage, returning ID of that instance
	 * @param instanceName name of current application instance. Will be used to mark written data
	 * @return ID of application instance as recorded in storage
	 * @throws CradleStorageException if storage initialization failed
	 */
	protected abstract String doInit(String instanceName) throws CradleStorageException;
	protected abstract void doDispose() throws CradleStorageException;
	
	
	protected abstract void doStoreMessageBatch(StoredMessageBatch batch) throws IOException;
	protected abstract void doStoreTimeMessage(StoredMessage message) throws IOException;
	protected abstract void doStoreProcessedMessageBatch(StoredMessageBatch batch) throws IOException;
	protected abstract void doStoreTestEvent(StoredTestEvent event) throws IOException;
	protected abstract void doStoreTestEventMessagesLink(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messagesIds) throws IOException;
	protected abstract StoredMessage doGetMessage(StoredMessageId id) throws IOException;
	protected abstract StoredMessage doGetProcessedMessage(StoredMessageId id) throws IOException;
	protected abstract long doGetLastMessageIndex(String streamName, Direction direction) throws IOException;
	protected abstract StoredMessageId doGetFirstMessageId(Instant seconds, String streamName, Direction direction) throws IOException;
	protected abstract Iterable<StoredMessageId> doGetFirstMessageIds(Instant seconds) throws IOException;
	protected abstract StoredTestEventWrapper doGetTestEvent(StoredTestEventId id) throws IOException;
	
	
	/**
	 * TestEventsMessagesLinker is used to obtain links between test events and messages
	 * @return instance of TestEventsMessagesLinker
	 */
	public abstract TestEventsMessagesLinker getTestEventsMessagesLinker();
	
	protected abstract Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException;
	protected abstract Iterable<StoredTestEventWrapper> doGetRootTestEvents() throws IOException;
	protected abstract Iterable<StoredTestEventWrapper> doGetTestEvents(StoredTestEventId parentId) throws IOException;
	
	
	/**
	 * Initializes storage, i.e. creates needed streams and gets ready to write data marked with given instance name
	 * @param instanceName name of current application instance. Will be used to mark written data
	 * @throws CradleStorageException if storage initialization failed
	 */
	public final void init(String instanceName) throws CradleStorageException
	{
		if (workingState)
			throw new CradleStorageException("Already initialized");
		
		logger.info("Storage initialization started");
		instanceId = doInit(instanceName);
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
	 * Writes data about given message batch to storage. Messages from batch are linked with corresponding streams
	 * @param batch data to write
	 * @throws IOException if data writing failed
	 */
	public final void storeMessageBatch(StoredMessageBatch batch) throws IOException
	{
		logger.debug("Storing message batch {}", batch.getId());
		doStoreMessageBatch(batch);
		storeTimeMessages(batch);
		logger.debug("Message batch {} has been stored", batch.getId());
	}
	
	/**
	 * Writes data about given processed message batch to storage. Messages from batch are linked with corresponding streams
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
	 * Writes data about given test event to storage.
	 * @param event data to write.
	 * @throws IOException if data writing failed
	 */
	public final void storeTestEvent(StoredTestEvent event) throws IOException
	{
		logger.debug("Storing test event {}", event.getId());
		try
		{
			TestEventUtils.validateTestEvent(event);
		}
		catch (CradleStorageException e)
		{
			throw new IOException("Invalid test event", e);
		}
		doStoreTestEvent(event);
		logger.debug("Test event {} has been stored", event.getId());
	}
	
	/**
	 * Writes to storage the links between given test event and messages.
	 * @param eventId ID of stored test event
	 * @param batchId ID of batch where event is stored, if applicable
	 * @param messagesIds collection of stored message IDs
	 * @throws IOException if data writing failed
	 */
	public final void storeTestEventMessagesLink(StoredTestEventId eventId, StoredTestEventId batchId, Collection<StoredMessageId> messagesIds) throws IOException
	{
		logger.debug("Storing link between test event {} and {} message(s)", eventId, messagesIds.size());
		doStoreTestEventMessagesLink(eventId, batchId, messagesIds);
		logger.debug("Link between test event {} and {} message(s) has been stored", eventId, messagesIds.size());
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
	 * Retrieves last stored message index for given stream and direction. Use result of this method to continue sequence of message indices.
	 * Indices are scoped by stream and direction, so both arguments are required 
	 * @param streamName to get message index for 
	 * @param direction to get message index for
	 * @return last stored message index for given arguments
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
	 * Retrieves ID of first message appeared in given second as measured from Epoch start
	 * @param seconds number of seconds passed since Epoch start
	 * @param streamName to which the message should be related
	 * @param direction of message
	 * @return ID of first message stored in given second
	 * @throws IOException if data retrieval failed
	 */
	public final StoredMessageId getFirstMessageId(Instant seconds, String streamName, Direction direction) throws IOException
	{
		seconds = Instant.ofEpochSecond(seconds.getEpochSecond());  //Cutting instant to have only seconds since Epoch
		logger.debug("Getting ID of first message appeared on {} for stream '{}' and direction '{}'", seconds, streamName, direction.getLabel());
		StoredMessageId result = doGetFirstMessageId(seconds, streamName, direction);
		logger.debug("First message ID appeared on {} for stream '{}' and direction '{}' got", seconds, streamName, direction.getLabel());
		return result;
	}
	
	/**
	 * Retrieves IDs of first messages (by stream and direction) appeared in given second as measured from Epoch start
	 * @param seconds number of seconds passed since Epoch start
	 * @return iterable object to enumerate IDs of first messages appeared in given second
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredMessageId> getFirstMessageIds(Instant seconds) throws IOException
	{
		seconds = Instant.ofEpochSecond(seconds.getEpochSecond());  //Cutting instant to have only seconds since Epoch
		logger.debug("Getting IDs of first messages appeared on {}", seconds);
		Iterable<StoredMessageId> result = doGetFirstMessageIds(seconds);
		logger.debug("IDs of first messages appeared on {} got", seconds);
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
	 * Allows to enumerate root test events
	 * @return iterable object to enumerate root test events
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getRootTestEvents() throws IOException
	{
		logger.debug("Getting root test events");
		Iterable<StoredTestEventWrapper> result = doGetRootTestEvents();
		logger.debug("Prepared iterator for root test events");
		return result;
	}
	
	/**
	 * Allows to enumerate children of test event with given ID
	 * @param parentId ID of parent test event
	 * @return iterable object to enumerate test events
	 * @throws IOException if data retrieval failed
	 */
	public final Iterable<StoredTestEventWrapper> getTestEvents(StoredTestEventId parentId) throws IOException
	{
		logger.debug("Getting children test events of {}", parentId);
		Iterable<StoredTestEventWrapper> result = doGetTestEvents(parentId);
		logger.debug("Prepared iterator for children test events of {}", parentId);
		return result;
	}
	
	
	protected void storeTimeMessages(StoredMessageBatch batch) throws IOException
	{
		Instant ts = null;
		for (StoredMessage msg : batch.getMessages())
		{
			Instant msgSeconds = TimeUtils.cutNanos(msg.getTimestamp());
			if (!msgSeconds.equals(ts))
			{
				ts = msgSeconds;
				doStoreTimeMessage(msg);
			}
		}
	}
}