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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.reports.ReportsMessagesLinker;
import com.exactpro.cradle.reports.StoredReport;
import com.exactpro.cradle.reports.StoredReportId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventsMessagesLinker;
import com.exactpro.cradle.utils.CradleStorageException;

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
	 * Writes contents of given report to storage
	 * @param report to store
	 * @throws IOException if data writing failed
	 */
	public abstract void storeReport(StoredReport report) throws IOException;
	
	/**
	 * Updates report in storage using report ID to find report to update.
	 * @param report to replace existing report. Report ID is used to find existing report
	 * @throws IOException if data writing failed
	 */
	public abstract void modifyReport(StoredReport report) throws IOException;
	
	
	/**
	 * Writes contents of given test event to storage, providing ID to find this record in future.
	 * @param testEvent to store. Event is bound to existing report by report ID. Additionally, event can be bound to another event by parentId
	 * @throws IOException if data writing failed
	 */
	public abstract void storeTestEvent(StoredTestEvent testEvent) throws IOException;
	
	/**
	 * Updates test event in storage using event ID to find event to update.
	 * @param testEvent to replace existing event. Event ID is used to find existing event
	 * @throws IOException if data writing failed
	 */
	public abstract void modifyTestEvent(StoredTestEvent testEvent) throws IOException;
	
	
	/**
	 * Writes to storage links between given report and messages.
	 * @param reportId ID of stored report
	 * @param messagesIds list of stored message IDs
	 * @throws IOException if data writing failed
	 */
	public abstract void storeReportMessagesLink(StoredReportId reportId, Set<StoredMessageId> messagesIds) throws IOException;
	
	/**
	 * Writes to storage links between given test event and messages.
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
	 * Retrieves report data stored under given ID
	 * @param id of stored report to retrieve
	 * @return data of stored report
	 * @throws IOException if report data retrieval failed
	 */
	public abstract StoredReport getReport(StoredReportId id) throws IOException;
	
	/**
	 * Retrieves test event data stored under given ID and related to given report ID
	 * @param reportId of stored report whose test event to retrieve
	 * @param id of stored test event to retrieve
	 * @return data of stored test event
	 * @throws IOException if test event data retrieval failed
	 */
	public abstract StoredTestEvent getTestEvent(StoredReportId reportId, StoredTestEventId id) throws IOException;
	
	
	/**
	 * StreamsMessagesLinker is used to obtain messages by stream
	 * @return new instance of StreamsMessagesLinker
	 */
	public abstract StreamsMessagesLinker getStreamsMessagesLinker();
	
	/**
	 * ReportsMessagesLinker is used to obtain links between reports and messages
	 * @return new instance of ReportsMessagesLinker
	 */
	public abstract ReportsMessagesLinker getReportsMessagesLinker();
	
	/**
	 * TestEventsMessagesLinker is used to obtain links between test events and messages
	 * @return new instance of TestEventsMessagesLinker
	 */
	public abstract TestEventsMessagesLinker getTestEventsMessagesLinker();
	
	
	/**
	 * Allows to enumerate stored messages
	 * @return iterable object to enumerate messages
	 * @throws IOException if data retrieval failed
	 */
	public abstract Iterable<StoredMessage> getMessages() throws IOException;
	
	/**
	 * Allows to enumerate stored reports
	 * @return iterable object to enumerate reports
	 * @throws IOException if data retrieval failed
	 */
	public abstract Iterable<StoredReport> getReports() throws IOException;
	
	/**
	 * Allows to enumerate stored test events linked to given report ID
	 * @param reportId of test events to obtain
	 * @return iterable object to enumerate test events linked with given report ID
	 * @throws IOException if data retrieval failed
	 */
	public abstract Iterable<StoredTestEvent> getReportTestEvents(StoredReportId reportId) throws IOException;
	
	
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