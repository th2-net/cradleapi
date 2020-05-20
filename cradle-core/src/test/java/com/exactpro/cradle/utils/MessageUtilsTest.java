/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.utils;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

public class MessageUtilsTest
{
	private StoredMessageBatch batch;
	private StoredMessage msg1,
			msg2;
	
	@BeforeClass
	public void prepare() throws CradleStorageException
	{
		MessageToStoreBuilder builder = new MessageToStoreBuilder();
		String streamName = "Stream1";
		Direction direction = Direction.FIRST;
		Instant timestamp = Instant.now();
		
		long index = 10;
		batch = new StoredMessageBatch();
		msg1 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.index(10)
				.timestamp(timestamp)
				.build());
		
		msg2 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.index(index+10)  //Need to have a gap between indices to verify that messages are written/read correctly
				.timestamp(timestamp)
				.build());
	}
	
	@Test
	public void messageIds() throws IOException
	{
		byte[] bytes = MessageUtils.serializeMessages(batch.getMessages());
		
		Collection<StoredMessage> restored = MessageUtils.deserializeMessages(bytes);
		Iterator<StoredMessage> it = restored.iterator();
		
		Assert.assertEquals(it.next().getId(), msg1.getId(), "1st message ID");
		Assert.assertEquals(it.next().getId(), msg2.getId(), "2nd message ID");
	}
	
	@Test
	public void oneMessageId() throws IOException
	{
		byte[] bytes = MessageUtils.serializeMessages(batch.getMessages());
		StoredMessage restored = MessageUtils.deserializeOneMessage(bytes, msg2.getId());
		Assert.assertEquals(restored.getId(), msg2.getId(), "ID of requested message");
	}
}
