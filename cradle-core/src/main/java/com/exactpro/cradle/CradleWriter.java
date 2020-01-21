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
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper for data writing operations related to {@link CradleStorage}.
 */
public class CradleWriter
{
	private final CradleStorage storage;
	
	public CradleWriter(CradleStorage storage)
	{
		this.storage = storage;
	}
	
	/**
	 * Stores data about sent message
	 * @param message to store
	 * @param metadata additional message data to store, if supported by storage implementation
	 * @param sender object that have sent this message. Data about sender will be linked with stored message
	 * @return ID of record in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public StoredMessageId storeSentMessage(byte[] message, Map<String, Object> metadata, CradleStream sender) throws IOException
	{
		StoredMessage sm = createStoredMessage(message, metadata, sender, Direction.SENT);
		return storage.storeMessage(sm);
	}
	
	/**
	 * Stores data about received message
	 * @param message to store
	 * @param metadata additional message data to store, if supported by storage implementation
	 * @param receiver stream that have received this message. Data about receiver will be linked with stored message
	 * @return ID of record in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public StoredMessageId storeReceivedMessage(byte[] message, Map<String, Object> metadata, CradleStream receiver) throws IOException
	{
		StoredMessage sm = createStoredMessage(message, metadata, receiver, Direction.RECEIVED);
		return storage.storeMessage(sm);
	}
	
	/**
	 * Stores automation report found by given path
	 * @param reportPath path to automation report to store
	 * @param matrixName name of matrix which produced the report
	 * @return ID of record in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public String storeReport(Path reportPath, String matrixName) throws IOException
	{
		return storage.storeReport(reportPath, matrixName);
	}

	/**
	 * Stores the links of messages and related report
	 * @param reportId id of stored report
	 * @param messagesIds ids of stored messages
	 * @return list of record ID in storage to find written data
	 * @throws IOException if data writing failed
	 */
	public List<String> storeReportMessagesLink(String reportId, Set<StoredMessageId> messagesIds) throws IOException
	{
		return storage.storeReportMessagesLink(reportId, messagesIds);
	}

	
	protected StoredMessage createStoredMessage(byte[] message, Map<String, Object> metadata, CradleStream stream, Direction direction)
	{
		StoredMessageBuilder builder = new StoredMessageBuilder();
		
		return builder.message(message)
				.direction(direction)
				.streamName(stream.getName())
				.timestamp(Instant.now())
				.build();
	}
}
