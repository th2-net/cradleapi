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
import java.util.List;

import com.exactpro.cradle.messages.StoredMessageId;

public interface StreamsMessagesLinker
{
	/**
	 * Retrieves IDs of stored messages that have went through the stream
	 * @param streamName name of stream
	 * @return list of stored message IDs
	 * @throws IOException if messages data retrieval failed
	 */
	List<StoredMessageId> getMessageIdsOfStream(String streamName) throws IOException;

	/**
	 * Checks if any messages have went through the stream
	 * @param streamName name of stream
	 * @return true if messages have went through stream, false otherwise
	 * @throws IOException if messages data retrieval failed
	 */
	boolean isStreamLinkedToMessages(String streamName) throws IOException;
}
