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
import java.time.Instant;
import java.util.Map;

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
	
	
	protected StoredMessage createStoredMessage(byte[] message, Map<String, Object> metadata, CradleStream stream, Direction direction)
	{
		StoredMessageBuilder builder = new StoredMessageBuilder();
		
		return builder.content(message)
				.direction(direction)
				.streamName(stream.getName())
				.timestamp(Instant.now())
				.build();
	}
}
