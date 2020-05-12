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

import java.time.Instant;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleStorageException;

public class StoredMessageBatchTest
{
	private MessageToStoreBuilder builder;
	private String streamName;
	private Direction direction;
	
	@BeforeClass
	public void prepare()
	{
		builder = new MessageToStoreBuilder();
		streamName = "Stream1";
		direction = Direction.FIRST;
	}
	
	@DataProvider(name = "first message")
	public Object[][] firtMessage()
	{
		return new Object[][]
				{
					{null, direction, 0},
					{streamName, null, 0},
					{streamName, direction, -1}
				};
	}
	
	@DataProvider(name = "multiple messages")
	public Object[][] multipleMessages()
	{
		Direction d = Direction.FIRST;
		long index = 1;
		return new Object[][]
				{
					{streamName, d, index, streamName+"X", d, index+1},
					{streamName, d, index, streamName, Direction.SECOND, index+1},
					{streamName, d, index, streamName, d, 1}
				};
	}
	
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch is full")
	public void batchIsLimited() throws CradleStorageException
	{
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(1)
				.build());
		for (int i = 0; i <= StoredMessageBatch.MAX_MESSAGES_NUMBER; i++)
			batch.addMessage(builder
					.streamName(streamName)
					.direction(direction)
					.build());
	}
	
	public void batchisFull() throws CradleStorageException
	{
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(1)
				.build());
		for (int i = 0; i < StoredMessageBatch.MAX_MESSAGES_NUMBER; i++)
			batch.addMessage(builder
					.streamName(streamName)
					.direction(direction)
					.build());
		
		Assert.assertEquals(batch.isFull(), true);
	}
	
	@Test(dataProvider = "first message",
			expectedExceptions = {CradleStorageException.class}, 
			expectedExceptionsMessageRegExp = ".* for first message in batch .*")
	public void batchChecksFirstMessage(String streamName, Direction direction, long index) throws CradleStorageException
	{
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(index)
				.build());
	}
	
	@Test(dataProvider = "multiple messages",
			expectedExceptions = {CradleStorageException.class},
			expectedExceptionsMessageRegExp = ".*, but in your message it is .*")
	public void batchConsistency(String streamName, Direction direction, long index, 
			String streamName2, Direction direction2, long index2) throws CradleStorageException
	{
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(index)
				.build());
		
		batch.addMessage(builder
				.streamName(streamName2)
				.direction(direction2)
				.index(index2)
				.build());
	}
	
	@Test
	public void indexAutoIncrement() throws CradleStorageException
	{
		long index = 10;
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(index)
				.build());
		
		StoredMessage msg = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.build());
		
		Assert.assertEquals(msg.getIndex(), index+1);
	}
	
	@Test
	public void batchShowsLastTimestamp() throws CradleStorageException
	{
		Instant timestamp = Instant.ofEpochSecond(1000);
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(1)
				.timestamp(timestamp)
				.build());
		
		batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp.plusSeconds(10))
				.build());
		
		Instant lastTimestamp = batch.getLastTimestamp();
		
		batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp.plusSeconds(20))
				.build());
		
		Assert.assertNotEquals(batch.getLastTimestamp(), lastTimestamp);
	}
}
