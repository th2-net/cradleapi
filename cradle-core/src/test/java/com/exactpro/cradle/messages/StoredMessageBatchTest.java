/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.messages;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.serialization.SerializationUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;

public class StoredMessageBatchTest
{
	private MessageToStoreBuilder builder;
	private String streamName;
	private Direction direction;
	private Instant timestamp;
	private byte[] messageContent;
	
	@BeforeClass
	public void prepare()
	{
		builder = new MessageToStoreBuilder();
		streamName = "Stream1";
		direction = Direction.FIRST;
		timestamp = Instant.now();
		messageContent = "Message text".getBytes();
	}
	
	@DataProvider(name = "first message")
	public Object[][] firstMessage()
	{
		return new Object[][]
				{
					{"", direction, 0},
					//{streamName, null, 0},  - this is invalid message, so skipping it
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
	
	@DataProvider(name = "invalid messages")
	public Object[][] invalidMessages()
	{
		return new Object[][]
				{
					{builder.build()},                                                                      //Empty message
					{builder.streamName(streamName).direction(null).timestamp(null).build()},               //Only stream is set
					{builder.streamName(streamName).direction(direction).timestamp(null).build()},          //Only stream and direction are set
					{builder.streamName(streamName).direction(direction).timestamp(Instant.now()).build()}  //Content is not set
				};
	}
	
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch has not enough space to hold given message")
	public void batchContentIsLimited() throws CradleStorageException
	{
		byte[] content = new byte[5000];
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(1)
				.timestamp(timestamp)
				.content(content)
				.build());
		for (int i = 0; i <= (StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE/content.length)+1; i++)
			batch.addMessage(builder
					.streamName(streamName)
					.direction(direction)
					.timestamp(timestamp)
					.content(content)
					.build());
	}
	
	@Test
	public void batchIsFull() throws CradleStorageException
	{
		byte[] content = new byte[StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE/2];
		
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(1)
				.timestamp(timestamp)
				.content(content)
				.build());
		batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp)
				.content(new byte[(int) (StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE - batch.getBatchSize() - 34)]) // 30 (msg md) + 4 (len)
				.build());

		Assert.assertTrue(batch.isFull(), "Batch indicates it is full");
	}
	
	@Test
	public void batchCountsSpaceLeft() throws CradleStorageException
	{
		byte[] content = new byte[StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE / 2];
		StoredMessageBatch batch = new StoredMessageBatch();
		long left = batch.getSpaceLeft();

		MessageToStore build = builder
				.streamName(streamName)
				.direction(direction)
				.index(1)
				.timestamp(timestamp)
				.content(content)
				.build();

		batch.addMessage(build);
		
		long expected = left - MessagesSizeCalculator.calculateMessageSizeInBatch(build) - streamName.length();
		
		Assert.assertEquals(batch.getSpaceLeft(), expected, "Batch counts space left");
	}
	
	@Test
	public void batchChecksSpaceLeft() throws CradleStorageException
	{
		byte[] content = new byte[StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE / 2];
		StoredMessageBatch batch = new StoredMessageBatch();
		
		MessageToStore msg = builder
				.streamName(streamName)
				.direction(direction)
				.index(1)
				.timestamp(timestamp)
				.content(content)
				.build();
		batch.addMessage(msg);
		
		Assert.assertEquals(batch.hasSpace(msg), false, "Batch shows if it has space to hold given message");
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
				.timestamp(timestamp)
				.content(messageContent)
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
					.timestamp(timestamp)
					.content(messageContent)
					.build());
		}
	}
	
	@Test(dataProvider = "invalid messages",
			expectedExceptions = {CradleStorageException.class},
			expectedExceptionsMessageRegExp = "Message must .*")
	public void batchValidatesMessages(MessageToStore msg) throws CradleStorageException
	{
		StoredMessageBatch batch = new StoredMessageBatch();
		batch.addMessage(msg);
	}
	
	@Test
	public void indexAutoIncrement() throws CradleStorageException
	{
		long index = 10;
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(index)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		
		StoredMessage msg1 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		
		StoredMessage msg2 = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp)
				.content(messageContent)
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
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		
		batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.index(index+10)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
	}
	
	@Test
	public void correctMessageId() throws CradleStorageException
	{
		long index = 10;
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(index)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		StoredMessage msg = batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		Assert.assertEquals(msg.getId(), new StoredMessageId(streamName, direction, index+1));
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
				.content(messageContent)
				.build());
		
		batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp.plusSeconds(10))
				.content(messageContent)
				.build());
		
		Instant lastTimestamp = batch.getLastTimestamp();
		
		batch.addMessage(builder
				.streamName(streamName)
				.direction(direction)
				.timestamp(timestamp.plusSeconds(20))
				.content(messageContent)
				.build());
		
		Assert.assertNotEquals(batch.getLastTimestamp(), lastTimestamp, "Last timestamp is from last added message");
	}
	
	@Test
	public void batchSerialization() throws CradleStorageException, IOException
	{
		StoredMessageBatch batch = StoredMessageBatch.singleton(builder
				.streamName(streamName)
				.direction(direction)
				.index(0)
				.timestamp(timestamp)
				.metadata("md", "some value")
				.content(messageContent)
				.build());
		StoredMessage storedMsg = batch.getFirstMessage();
		byte[] bytes = MessageUtils.serializeMessages(batch.getMessages());
		StoredMessage msg = MessageUtils.deserializeMessages(bytes).iterator().next();
		Assert.assertEquals(msg, storedMsg, "Message should be completely serialized/deserialized");
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

	@Test
	public void streamNameAffectsBatchLength() throws CradleStorageException, IOException
	{
		String shortStreamName = "strNm";
		String longStreamName = "s".repeat(SerializationUtils.USHORT_MAX_VALUE);
		MessageToStore shortMsg = builder.streamName(shortStreamName).direction(direction).index(0).timestamp(timestamp)
				.metadata("md", "some value").content(messageContent).build();
		MessageToStore longMsg = builder.streamName(longStreamName).direction(direction).index(0).timestamp(timestamp)
				.metadata("md", "some value").content(messageContent).build();
		StoredMessageBatch batch = StoredMessageBatch.singleton(shortMsg);
		StoredMessageBatch batch2 = StoredMessageBatch.singleton(longMsg);

		Assert.assertTrue(batch.getBatchSize() < batch2.getBatchSize());
		Assert.assertEquals(batch2.getBatchSize() - batch.getBatchSize(), longStreamName.length() - shortStreamName.length());
	}

	@Test
	public void streamNameAffectsBatchLength2() throws CradleStorageException, IOException
	{
		String shortStreamName = "strNm";
		String longStreamName = "s".repeat(SerializationUtils.USHORT_MAX_VALUE);
		MessageToStore msgExample = builder.streamName(shortStreamName).direction(direction).index(0).timestamp(timestamp)
				.metadata("md", "some value").content(messageContent).build();
		int size = MessagesSizeCalculator.calculateMessageSizeInBatch(msgExample);
		StoredMessageBatch batch = fillBatch(shortStreamName);
		StoredMessageBatch batch2 = fillBatch(longStreamName);

		// (+ size - 1) / size = to round up integer dividing
		Assert.assertEquals(batch.getMessageCount() - batch2.getMessageCount(),
				((longStreamName.length() - shortStreamName.length()) + size - 1) / size);
	}

	private StoredMessageBatch fillBatch (String streamName) throws CradleStorageException {
		StoredMessageBatch batch = new StoredMessageBatch();
		boolean added;
		long index = 0;
		do {
			MessageToStore build = builder.streamName(streamName).direction(direction).index(index).timestamp(timestamp)
					.metadata("md", "some value").content(messageContent).build();
			added = batch.hasSpace(build);
			if (added)
				batch.addMessage(build);
			index++;
		} while (added);
		return batch;
	}

}
