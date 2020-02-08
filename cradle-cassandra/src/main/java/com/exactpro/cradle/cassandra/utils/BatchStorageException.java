/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.utils;

import java.util.List;

import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.utils.CradleStorageException;

public class BatchStorageException extends CradleStorageException
{
	private static final long serialVersionUID = -4452963436263568944L;
	
	private final List<StoredMessage> messages;
	
	public BatchStorageException(String message, List<StoredMessage> storedMessages)
	{
		super(message);
		this.messages = storedMessages;
		
	}
	
	public BatchStorageException(String message, Throwable cause, List<StoredMessage> storedMessages)
	{
		super(message, cause);
		this.messages = storedMessages;
	}
	
	
	public List<StoredMessage> getMessages()
	{
		return messages;
	}
}
