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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Storage which holds information about all data sent or verified and generated reports.
 */
public abstract class CradleStorage
{
	private static final Logger logger = LoggerFactory.getLogger(CradleStorage.class);
	
	private String instanceId;
	private Map<String, String> streamsById,
			streamsByName;
	
	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
	private final Lock getStreamLock = rwLock.readLock();
	private final Lock modifyStreamLock = rwLock.writeLock();

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
	 * Writes data about given message to storage, providing ID to find this record in future
	 * @param message data to write
	 * @return ID of record in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public abstract StoredMessageId storeMessage(StoredMessage message) throws IOException;
	
	/**
	 * Writes contents of given report to storage, providing ID to find this record in future.
	 * @param report to store
	 * @return ID of record in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public abstract String storeReport(StoredReport report) throws IOException;
	
	/**
	 * Updates report in storage using report ID to find report to update.
	 * @param report to replace existing report. Report ID is used to find existing report
	 * @throws IOException if data writing failed
	 */
	public abstract void modifyReport(StoredReport report) throws IOException;
	
	/**
	 * Writes links of given report and messages to storage, providing IDs to find this records in
	 * future
	 * @param reportId ID of stored report
	 * @param messagesIds list of stored message IDs
	 * @return list of record IDs in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public abstract List<String> storeReportMessagesLink(String reportId, Set<StoredMessageId> messagesIds) throws IOException;

	/**
	 * Retrieves ID of stored report by linked message ID
	 * @param messageId ID of stored message
	 * @return ID of stored report
	 * @throws IOException if report data retrieval failed
	 */
	public abstract String getReportIdByMessageId(String messageId) throws IOException;

	/**
	 * Retrieves IDs of stored messages by linked report ID
	 * @param reportId ID of stored report
	 * @return list of stored message IDs
	 * @throws IOException if messages data retrieval failed
	 */
	public abstract List<String> getMessageIdsByReportId(String reportId) throws IOException;

	/**
	 * Check if report has messages related to it
	 * @param reportId ID of stored report
	 * @return true - if report has related messages, false otherwise
	 * @throws IOException if messages data retrieval failed
	 */
	public abstract boolean doMessagesRelatedToReportExist(String reportId) throws IOException;

	/**
	 * Retrieves messages data stored under given ID
	 * @param rowId ID of database row to retrieve messages data from
	 * @return list of data of stored messages
	 * @throws IOException if messages data retrieval failed
	 */
	public abstract Collection<StoredMessage> getMessages(String rowId) throws IOException;

	/**
	 * Retrieves report data stored under given ID
	 * @param id of stored report to retrieve
	 * @return data of stored report
	 * @throws IOException if report data retrieval failed
	 */
	public abstract StoredReport getReport(String id) throws IOException;
	
	/**
	 * Makes query to storage to obtain ID of stream with given name
	 * @param streamName name of stream to get ID for
	 * @return ID of stream as recorded in storage or null if that stream is not recorded in storage
	 * @throws IOException if data retrieval failed
	 */
	protected abstract String queryStreamId(String streamName) throws IOException;
	
	/**
	 * Stores data about given stream
	 * @param stream data to store
	 * @return ID of record in storage to find written data
	 * @throws IOException if data writing failed
	 */
	protected abstract String doStoreStream(CradleStream  stream) throws IOException;
	
	/**
	 * Updates stream data stored under given ID with new stream data
	 * @param id of stream data to update
	 * @param newStream data to update with
	 * @throws IOException if data writing failed
	 */
	protected abstract void doModifyStream(String id, CradleStream  newStream) throws IOException;
	
	/**
	 * Updates stream name stored under given ID with new name
	 * @param id of stream to update
	 * @param newName to update with
	 * @throws IOException if data writing failed
	 */
	protected abstract void doModifyStreamName(String id, String newName) throws IOException;
	
	
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
		streamsById = new HashMap<>();
		streamsByName = new HashMap<>();
	}
	
	/**
	 * Switches storage from its initial state to working state. This affects storage operations.
	 * For example, in initial state {@link #storeStream(CradleStream stream) storeStream} checks if stream was already stored.
	 * This is useful while initializing streams on application startup.
	 * In working state new stream is simply added to storage, i.e. is considered as new one.
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
	
	/**
	 * Stores data about given stream
	 * @param stream data to store
	 * @return ID of record in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public String storeStream(CradleStream stream) throws IOException
	{
		if (!workingState)
		{
			String id = queryStreamId(stream.getName());  //Stream known to application, but unknown to storage
			if (id == null)
				return actuallyStoreStream(stream);
			
			addStreamData(id, stream);
			return id;
		}
		
		String name = stream.getName(),
				id = getStreamId(name);
		if (id != null)
		{
			logger.warn("Refused to store stream with already existing name '"+name+"'");
			return null;
		}
		
		return actuallyStoreStream(stream);
	}
	
	/**
	 * Updates stream data recorded in storage with new stream data
	 * @param oldStream stream data to update
	 * @param newStream data to update with
	 * @throws IOException if data writing failed
	 */
	public void modifyStream(CradleStream oldStream, CradleStream newStream) throws IOException
	{
		String name = oldStream.getName(),
				id = getStreamId(name);
		if (id == null)
		{
			logger.warn("Refused to modify unknown stream '"+name+"'");
			return;
		}
		
		modifyStreamLock.lock();
		try
		{
			String newName = newStream.getName();
			if (!name.equals(newName))
				changeStreamName(id, name, newName);
			doModifyStream(id, newStream);
		}
		finally
		{
			modifyStreamLock.unlock();
		}
	}
	
	/**
	 * Updates stream name recorded in storage with new name
	 * @param oldName of stream to update
	 * @param newName to update with
	 * @throws IOException if data writing failed
	 */
	public void renameStream(String oldName, String newName) throws IOException
	{
		if (oldName.equals(newName))
			return;
		
		String id = getStreamId(oldName);
		if (id == null)
		{
			logger.warn("Refused to rename unknown stream '"+oldName+"'");
			return;
		}
		
		modifyStreamLock.lock();
		try
		{
			changeStreamName(id, oldName, newName);
			doModifyStreamName(id, newName);
		}
		finally
		{
			modifyStreamLock.unlock();
		}
	}
	
	/**
	 * Returns name of stream stored under given ID
	 * @param id of stream
	 * @return name of stream
	 */
	public String getStreamName(String id)
	{
		getStreamLock.lock();
		try
		{
			return streamsById.get(id);
		}
		finally
		{
			getStreamLock.unlock();
		}
	}
	
	/**
	 * Returns ID of stream with given name as recorded in storage
	 * @param name of stream
	 * @return ID of stream
	 */
	public String getStreamId(String name)
	{
		getStreamLock.lock();
		try
		{
			return streamsByName.get(name);
		}
		finally
		{
			getStreamLock.unlock();
		}
	}
	
	
	private void changeStreamName(String id, String oldName, String newName)
	{
		//modifyStreamLock is switched on outside of this method
		streamsById.put(id, newName);
		streamsByName.remove(oldName);
		streamsByName.put(newName, id);
	}
	
	private String actuallyStoreStream(CradleStream stream) throws IOException
	{
		modifyStreamLock.lock();
		try
		{
			String id = doStoreStream(stream),
					name = stream.getName();
			streamsById.put(id, name);
			streamsByName.put(name, id);
			return id;
		}
		finally
		{
			modifyStreamLock.unlock();
		}
	}
	
	private void addStreamData(String id, CradleStream stream)
	{
		modifyStreamLock.lock();
		try
		{
			String name = stream.getName();
			streamsById.put(id, name);
			streamsByName.put(name, id);
		}
		finally
		{
			modifyStreamLock.unlock();
		}
	}
}