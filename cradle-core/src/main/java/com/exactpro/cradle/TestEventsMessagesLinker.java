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

public interface TestEventsMessagesLinker
{
	/**
	 * Retrieves ID of stored test event by linked message ID
	 * @param messageId ID of stored message
	 * @return ID of stored test event 
	 * @throws IOException if test event data retrieval failed
	 */
	String getTestEventIdByMessageId(StoredMessageId messageId) throws IOException;

	/**
	 * Retrieves IDs of stored messages by linked test event ID
	 * @param eventId ID of stored test event
	 * @return list of stored message IDs
	 * @throws IOException if messages data retrieval failed
	 */
	List<StoredMessageId> getMessageIdsByEventId(String eventId) throws IOException;

	/**
	 * Checks if test event has messages linked to it
	 * @param eventId ID of stored test event
	 * @return true if test event has linked messages, false otherwise
	 * @throws IOException if messages data retrieval failed
	 */
	boolean isTestEventLinkedToMessages(String eventId) throws IOException;
}
