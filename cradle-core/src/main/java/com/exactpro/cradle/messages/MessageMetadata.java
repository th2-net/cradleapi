/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.messages;

/**
 * Metadata of message to store in Cradle
 */
public class MessageMetadata extends StoredMessageMetadata
{
	public MessageMetadata()
	{
	}
	
	public MessageMetadata(MessageMetadata metadata)
	{
		super(metadata);
	}
	
	
	public void add(String key, String value)
	{
		data.put(key, value);
	}
}
