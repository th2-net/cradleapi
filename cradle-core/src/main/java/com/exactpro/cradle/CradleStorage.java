/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle;

import java.io.IOException;
import java.util.*;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.testevents.TestEventsParentsLinker;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Storage which holds information about all data sent or verified and generated reports.
 */
public abstract class CradleStorage
{
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
	
	/**
	 * Disposes resources occupied by storage which means closing of opened connections, flushing all buffers, etc.
	 * @throws CradleStorageException if there was error during storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	public abstract void dispose() throws CradleStorageException;
	
	
	/**
	 * Writes data about given message batch to storage. Messages from batch are linked with corresponding streams
	 * @param batch data to write
	 * @throws IOException if data writing failed
	 */
	public abstract void storeMessageBatch(StoredMessageBatch batch) throws IOException;
	
	
	/**
	 * Writes data about given test event batch to storage.
	 * @param batch data to write.
	 * @throws IOException if data writing failed
	 */
	public abstract void storeTestEventBatch(StoredTestEventBatch batch) throws IOException;
	
	
	/**
	 * Writes to storage the links between given test event and messages.
	 * @param eventId ID of stored test event
	 * @param messagesIds list of stored message IDs
	 * @throws IOException if data writing failed
	 */
	public abstract void storeTestEventMessagesLink(StoredTestEventId eventId, Set<StoredMessageId> messagesIds) throws IOException;
	
	
	/**
	 * Retrieves message data stored under given ID
	 * @param id of stored message to retrieve
	 * @return data of stored messages
	 * @throws IOException if message data retrieval failed
	 */
	public abstract StoredMessage getMessage(StoredMessageId id) throws IOException;

	/**
	 * Retrieves test event data stored under given ID
	 * @param id of stored test event to retrieve
	 * @return data of stored test event
	 * @throws IOException if test event data retrieval failed
	 */
	public abstract StoredTestEvent getTestEvent(StoredTestEventId id) throws IOException;
	
	
	/**
	 * StreamsMessagesLinker is used to obtain messages by stream
	 * @return instance of StreamsMessagesLinker
	 */
	public abstract StreamsMessagesLinker getStreamsMessagesLinker();
	
	/**
	 * TestEventsMessagesLinker is used to obtain links between test events and messages
	 * @return instance of TestEventsMessagesLinker
	 */
	public abstract TestEventsMessagesLinker getTestEventsMessagesLinker();
	
	/**
	 * TestEventsParentsLinker is used to obtain test events by their parent
	 * @return instance of TestEventsParentsLinker
	 */
	public abstract TestEventsParentsLinker getTestEventsParentsLinker();
	
	
	/**
	 * Allows to enumerate stored messages, optionally filtering them by given conditions
	 * @param filter defines conditions to filter messages by. Use null is no filtering is needed
	 * @return iterable object to enumerate messages
	 * @throws IOException if data retrieval failed
	 */
	public abstract Iterable<StoredMessage> getMessages(StoredMessageFilter filter) throws IOException;
	
	/**
	 * Allows to enumerate test events
	 * @param onlyRootEvents set to true if you need to obtain only root test events, i.e. events with no parent 
	 * @return iterable object to enumerate test events
	 * @throws IOException if data retrieval failed
	 */
	public abstract Iterable<StoredTestEvent> getTestEvents(boolean onlyRootEvents) throws IOException;
	
	
	/**
	 * Initializes storage, i.e. creates needed streams and gets ready to write data marked with given instance name
	 * @param instanceName name of current application instance. Will be used to mark written data
	 * @throws CradleStorageException if storage initialization failed
	 */
	public void init(String instanceName) throws CradleStorageException
	{
		if (workingState)
			throw new CradleStorageException("Already initialized");
		
		instanceId = doInit(instanceName);
	}
	
	/**
	 * Switches storage from its initial state to working state. This affects storage operations.
	 */
	public void initFinish()
	{
		workingState = true;
	}
	
	/**
	 * @return ID of current application instance as recorded in storage
	 */
	public String getInstanceId()
	{
		return instanceId;
	}
}