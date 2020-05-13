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
import java.util.Arrays;
import java.util.List;

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
	public Object[][] firstMessage()
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
		long index = 10;
		return new Object[][]
				{
					{Arrays.asList(new IdData(streamName, d, index), 
							new IdData(streamName+"X", d, index+1))},             //Different streams
					{Arrays.asList(new IdData(streamName, d, index), 
							new IdData(streamName, Direction.SECOND, index+1))},  //Different directions
					{Arrays.asList(new IdData(streamName, d, index), 
							new IdData(streamName, d, index),                     //Index is not incremented
							new IdData(streamName, d, index-1))}                  //Index is less than previous
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
	
	public void batchIsFull() throws CradleStorageException
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
		
		Assert.assertEquals(batch.isFull(), true, "Batch indicates it is full");
	}
	
	@Test(dataProvider = "first message",
			expectedExceptions = {CradleStorageException.class}, 
			expectedExceptionsMessageRegExp = ".* for first message in batch .*")
	public void batchChecksFirstMessage(String streamName, Direction direction, long index) throws CradleStorageException
	{
		StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(index)
				.build());
	}
	
	@Test(dataProvider = "multiple messages",
			expectedExceptions = {CradleStorageException.class},
			expectedExceptionsMessageRegExp = ".*, but in your message it is .*")
	public void batchConsistency(List<IdData> ids) throws CradleStorageException
	{
		StoredMessageBatch batch = new StoredMessageBatch();
		for (IdData id : ids)
		{
			batch.addMessage(builder
					.streamName(id.streamName)
					.direction(id.direction)
					.index(id.index)
					.build());
		}
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
		
		StoredMessage msg1 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.build());
		
		StoredMessage msg2 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.build());
		
		Assert.assertEquals(msg1.getIndex(), index+1, "1st and 2nd messages should have sequenced indices");
		Assert.assertEquals(msg2.getIndex(), msg1.getIndex()+1, "2nd and 3rd messages should have sequenced indices");
	}
	
	@Test
	public void indexGapsAllowed() throws CradleStorageException
	{
		long index = 10;
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(index)
				.build());
		
		batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.index(index+10)
				.build());
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
		
		Assert.assertNotEquals(batch.getLastTimestamp(), lastTimestamp, "Last timestamp is from last added message");
	}
	
	
	class IdData
	{
		String streamName;
		Direction direction;
		long index;
		
		public IdData(String streamName, Direction direction, long index)
		{
			this.streamName = streamName;
			this.direction = direction;
			this.index = index;
		}
		
		@Override
		public String toString()
		{
			return streamName+StoredMessageBatchId.IDS_DELIMITER+direction.getLabel()+StoredMessageBatchId.IDS_DELIMITER+index;
		}
	}
}
