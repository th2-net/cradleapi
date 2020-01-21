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

import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Interface which allows to go through messages stored in the storage
 */
public interface MessageNavigator
{
	/**
	 * Searches message in storage according to passed parameters and sets it as current message
	 * @param messageId ID of stored message
	 * @return found message
	 * @throws CradleStorageException if storage doesn't contain message for given parameters
	 */
	StoredMessage setCurrentMessage(String messageId) throws CradleStorageException;

	/**
	 * @return current message the navigator points to
	 */
	StoredMessage getCurrentMessage();

	/**
	 * Switches to message next to current one and makes it current message
	 * @return next message
	 * @throws CradleStorageException if there is no next message
	 */
	StoredMessage getNextMessage() throws CradleStorageException;

	/**
	 * Switches to message previous to current one and makes it current message
	 * @return previous message
	 * @throws CradleStorageException if there is no previous message
	 */
	StoredMessage getPrevMessage() throws CradleStorageException;

	/**
	 * @return ID of storage record where current message is stored
	 */
	String getCurrentRecordId();

	/**
	 * @return index of current message in storage record where message is stored
	 */
	int getCurrentMessageIndex();
}
