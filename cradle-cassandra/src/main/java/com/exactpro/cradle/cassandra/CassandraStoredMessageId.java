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

import com.exactpro.cradle.StoredMessageId;

/**
 * Holder for data to find message stored in CassandraCradleStorage.
 */
public class CassandraStoredMessageId extends StoredMessageId
{
	private static final long serialVersionUID = 6722783437389553280L;
	
	public static final String DELIMITER = ":";
	
	private String batchId;
	private int messageIndex;
	
	public CassandraStoredMessageId() {}
	
	/**
	 * @param id batch ID
	 * @param messageIndex index of stored message in batch
	 */
	public CassandraStoredMessageId(String id, int messageIndex)
	{
		this.batchId = id;
		this.messageIndex = messageIndex;
	}
	
	public CassandraStoredMessageId(String messageId)
	{
		super(messageId);
		String[] parts = messageId.split(DELIMITER);
		this.batchId = parts[0];
		this.messageIndex = Integer.parseInt(parts[1]);
	}
	
	@Override
	public String getId()
	{
		return batchId;
	}
	
	@Override
	public void setId(String id)
	{
		this.batchId = id;
	}
	
	
	//These two methods are mostly needed for serialization/deserialization to work
	public String getBatchId()
	{
		return batchId;
	}
	
	public void setBatchId(String batchId)
	{
		this.batchId = batchId;
	}
	
	
	public int getMessageIndex()
	{
		return messageIndex;
	}
	
	public void setMessageIndex(int messageIndex)
	{
		this.messageIndex = messageIndex;
	}
	
	
	@Override
	public String toString()
	{
		return getId()+DELIMITER+messageIndex;
	}
}
