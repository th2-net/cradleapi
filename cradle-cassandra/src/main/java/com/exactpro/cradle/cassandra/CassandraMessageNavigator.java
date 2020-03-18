/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.MessageNavigator;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.utils.CradleStorageException;

public class CassandraMessageNavigator implements MessageNavigator
{
	private final CassandraCradleStorage storage;
	private List<StoredMessage> batchMessages;
	private String batchId;
	private int messageIndex;

	public CassandraMessageNavigator(CassandraCradleStorage storage)
	{
		this.storage = storage;
	}
	
	@Override
	public String getCurrentRecordId()
	{
		return batchId;
	}
	
	@Override
	public int getCurrentMessageIndex()
	{
		return messageIndex;
	}
	
	@Override
	public StoredMessage getCurrentMessage()
	{
		if (batchMessages != null)
			return batchMessages.get(messageIndex);
		return null;
	}
	
	@Override
	public StoredMessage setCurrentMessage(String messageId) throws CradleStorageException
	{
		CassandraStoredMessageId id = new CassandraStoredMessageId(messageId);
		String recordId = id.getId();
		int messageIndex = id.getMessageIndex();
		List<StoredMessage> messages;
		if (!StringUtils.equals(recordId, batchId))
		{
			try
			{
				messages = storage.getMessages(recordId);
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Error while getting messages from storage for record ID '"+recordId+"'", e);
			}
		}
		else
			messages = batchMessages;
		
		if (messages == null)
			throw new CradleStorageException("Record ID is invalid or does not point to a stored record");
		
		if (messageIndex >= messages.size())
		{
			throw new CradleStorageException("Index of message ("+messageIndex+") is greater than number of messages " +
					"in current record (" + messages.size() + ")");
		}
		
		batchId = recordId;
		batchMessages = messages;
		this.messageIndex = id.getMessageIndex();
		
		return batchMessages.get(messageIndex);
	}


	@Override
	public StoredMessage getNextMessage() throws CradleStorageException
	{
		if (batchId == null) // Current message wasn't set, so we start from the very first message in storage
		{
			UUID firstBatchId = storage.getExtremeRecordId(CassandraCradleStorage.ExtremeFunction.MIN,
					storage.getSettings().getMessagesTableName(), StorageConstants.TIMESTAMP);
			if (firstBatchId == null) // Storage is empty now
				return null;
			
			return setCurrentMessage(firstBatchId + ":" + messageIndex);
		}
		else if (messageIndex + 1 < batchMessages.size())
		{
			messageIndex++;
		}
		else
		{
			String newId = storage.getNextRowId(batchId);
			if (newId == null)
				throw new CradleStorageException("Action not available: this is the last stored message");
			
			List<StoredMessage> messages;
			try
			{
				messages = storage.getMessages(newId);
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Error while getting messages from storage", e);
			}
			
			if (messages == null)
				throw new CradleStorageException("Next record is not available, record ID '"+newId+"'");
			
			batchId = newId;
			messageIndex = 0;
			batchMessages = messages;
		}
		
		return batchMessages.get(messageIndex);
	}

	@Override
	public StoredMessage getPrevMessage() throws CradleStorageException
	{
		if (messageIndex > 0)
			messageIndex--;
		else
		{
			String newId = storage.getPrevRowId(batchId);
			if (newId == null)
				throw new CradleStorageException("Action not available: this is the first stored message");
			
			List<StoredMessage> messages;
			try
			{
				messages = storage.getMessages(newId);
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Error while getting messages from storage", e);
			}
			
			if (messages == null)
				throw new CradleStorageException("Previous record is not available, record ID '"+newId+"'");
			
			batchId = newId;
			messageIndex = messages.size() - 1;
			batchMessages = messages;
		}
		
		return batchMessages.get(messageIndex);
	}
}
