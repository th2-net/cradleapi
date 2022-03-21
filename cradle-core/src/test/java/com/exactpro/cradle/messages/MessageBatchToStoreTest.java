/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

public class MessageBatchToStoreTest
{
	private final int MAX_SIZE = 1024*1024;
	
	private MessageToStoreBuilder builder;
	private BookId book;
	private String sessionAlias;
	private Direction direction;
	private Instant timestamp;
	private byte[] messageContent;
	
	@BeforeClass
	public void prepare()
	{
		builder = new MessageToStoreBuilder();
		book = new BookId("book1");
		sessionAlias = "Session1";
		direction = Direction.FIRST;
		timestamp = Instant.now().minus(1, ChronoUnit.DAYS);
		messageContent = "Message text".getBytes();
	}
	
	@DataProvider(name = "multiple messages")
	public Object[][] multipleMessages()
	{
		Direction d = Direction.FIRST;
		long seq = 10;
		return new Object[][]
				{
					{Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq), 
							new IdData(new BookId(book.getName()+"X"), sessionAlias, d, timestamp, seq+1))},             //Different books
					{Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq), 
							new IdData(book, sessionAlias+"X", d, timestamp, seq+1))},             //Different sessions
					{Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq), 
							new IdData(book, sessionAlias, Direction.SECOND, timestamp, seq+1))},  //Different directions
					{Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq), 
							new IdData(book, sessionAlias, Direction.FIRST, timestamp.plus(1, ChronoUnit.DAYS), seq+1))},  //Different date
					{Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq), 
							new IdData(book, sessionAlias, d, timestamp.minusMillis(1), seq))},    //Timestamp is less than previous
					{Arrays.asList(new IdData(book, sessionAlias, d, timestamp, seq), 
							new IdData(book, sessionAlias, d, timestamp, seq),                     //Sequence is not incremented
							new IdData(book, sessionAlias, d, timestamp, seq-1))}                  //Sequence is less than previous
				};
	}
	
	@DataProvider(name = "invalid messages")
	public Object[][] invalidMessages()
	{
		return new Object[][]
				{
					{builder},                                                                                       //Empty message
					{builder.bookId(null)},																			 //Null book
					{builder.bookId(new BookId(""))},														 //Empty book
					{builder.bookId(book)},                                                                          //Only book is set
					{builder.bookId(book).sessionAlias(null).direction(null).timestamp(null)},               		 //Null session is set
					{builder.bookId(book).sessionAlias("").direction(null).timestamp(null)},               		 	 //Empty session is set
					{builder.bookId(book).sessionAlias(sessionAlias).direction(null).timestamp(null)},               //Only book and session are set
					{builder.bookId(book).sessionAlias(sessionAlias).direction(direction).timestamp(null)},          //Only book, session and direction are set
					{builder.bookId(book).sessionAlias(sessionAlias).direction(direction).timestamp(Instant.now())}  //Content is not set
				};
	}
	
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch has not enough space to hold given message")
	public void batchContentIsLimited() throws CradleStorageException
	{
		byte[] content = new byte[5000];
		MessageBatchToStore batch = MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(1)
				.timestamp(timestamp)
				.content(content)
				.build(), MAX_SIZE);
		for (int i = 0; i <= (MAX_SIZE/content.length)+1; i++)
			batch.addMessage(builder
					.bookId(book)
					.sessionAlias(sessionAlias)
					.direction(direction)
					.timestamp(timestamp)
					.content(content)
					.build());
	}

	@Test
	public void batchIsFull1() throws CradleStorageException
	{
		MessageBatchToStore fullBySizeBatch = MessageBatchToStoreJoinTest.createFullBySizeBatch(book, sessionAlias,
				1, direction, timestamp);

		Assert.assertTrue(fullBySizeBatch.isFull(), "Batch indicates it is full");
		Assert.assertEquals(fullBySizeBatch.getBatchSize(), MessageBatchToStoreJoinTest.MAX_SIZE);
		Assert.assertEquals(fullBySizeBatch.getSpaceLeft(), 0);
	}

	@Test
	public void batchIsFull() throws CradleStorageException
	{
		byte[] content = new byte[MAX_SIZE/2];
		MessageBatchToStore batch = MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(1)
				.timestamp(timestamp)
				.content(content)
				.build(), MAX_SIZE);
		batch.addMessage(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.timestamp(timestamp)
				.content(new byte[MAX_SIZE - MessagesSizeCalculator.MESSAGE_BATCH_CONST_VALUE
						- MessagesSizeCalculator.MESSAGE_LENGTH_IN_BATCH * 2
						- MessagesSizeCalculator.MESSAGE_SIZE_CONST_VALUE * 2
						- content.length])
				.build());

		Assert.assertTrue(batch.isFull(), "Batch indicates it is full");
	}
	
	@Test
	public void batchCountsSpaceLeft() throws CradleStorageException
	{
		byte[] content = new byte[MAX_SIZE / 2];
		MessageBatchToStore batch = new MessageBatchToStore(MAX_SIZE);
		long left = batch.getSpaceLeft();

		MessageToStore msg = builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(1)
				.timestamp(timestamp)
				.content(content)
				.build();

		batch.addMessage(msg);
		
		Assert.assertEquals(batch.getSpaceLeft(), left - MessagesSizeCalculator.calculateMessageSizeInBatch(msg), "Batch counts space left");
	}
	
	@Test
	public void batchChecksSpaceLeft() throws CradleStorageException
	{
		byte[] content = new byte[MAX_SIZE/2];
		MessageBatchToStore batch = new MessageBatchToStore(MAX_SIZE);
		
		MessageToStore msg = builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(1)
				.timestamp(timestamp)
				.content(content)
				.build();
		batch.addMessage(msg);

		Assert.assertFalse(batch.hasSpace(msg), "Batch shows if it has space to hold given message");
	}
	
	
	@Test(expectedExceptions = {CradleStorageException.class}, 
			expectedExceptionsMessageRegExp = ".* for first message in batch .*")
	public void batchChecksFirstMessage() throws CradleStorageException
	{
		MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(-1)
				.timestamp(timestamp)
				.content(messageContent)
				.build(), MAX_SIZE);
	}
	
	@Test(dataProvider = "multiple messages",
			expectedExceptions = {CradleStorageException.class},
			expectedExceptionsMessageRegExp = ".*, but in your message it is .*")
	public void batchConsistency(List<IdData> ids) throws CradleStorageException
	{
		MessageBatchToStore batch = new MessageBatchToStore(MAX_SIZE);
		for (IdData id : ids)
		{
			batch.addMessage(builder
					.bookId(id.book)
					.sessionAlias(id.sessionAlias)
					.direction(id.direction)
					.sequence(id.sequence)
					.timestamp(id.timestamp)
					.content(messageContent)
					.build());
		}
	}
	
	@Test(dataProvider = "invalid messages",
			expectedExceptions = {CradleStorageException.class},
			expectedExceptionsMessageRegExp = "Message must .*")
	public void messageValidation(MessageToStoreBuilder builder) throws CradleStorageException
	{
		MessageBatchToStore batch = new MessageBatchToStore(MAX_SIZE);
		batch.addMessage(builder.build());
	}
	
	@Test
	public void sequenceAutoIncrement() throws CradleStorageException
	{
		long seq = 10;
		MessageBatchToStore batch = MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(seq)
				.timestamp(timestamp)
				.content(messageContent)
				.build(), MAX_SIZE);
		
		StoredMessage msg1 = batch.addMessage(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		
		StoredMessage msg2 = batch.addMessage(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		
		Assert.assertEquals(msg1.getSequence(), seq+1, "1st and 2nd messages should have ordered sequence numbers");
		Assert.assertEquals(msg2.getSequence(), msg1.getSequence()+1, "2nd and 3rd messages should have ordered sequence numbers");
	}
	
	@Test
	public void sequenceGapsAllowed() throws CradleStorageException
	{
		long seq = 10;
		MessageBatchToStore batch = MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(seq)
				.timestamp(timestamp)
				.content(messageContent)
				.build(), MAX_SIZE);
		
		batch.addMessage(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(seq+10)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
	}
	
	@Test
	public void correctMessageId() throws CradleStorageException
	{
		long seq = 10;
		MessageBatchToStore batch = MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(seq)
				.timestamp(timestamp)
				.content(messageContent)
				.build(), MAX_SIZE);
		StoredMessage msg = batch.addMessage(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.timestamp(timestamp)
				.content(messageContent)
				.build());
		Assert.assertEquals(msg.getId(), new StoredMessageId(book, sessionAlias, direction, timestamp, seq+1));
	}
	
	@Test
	public void batchShowsLastTimestamp() throws CradleStorageException
	{
		Instant timestamp = Instant.ofEpochSecond(1000);
		MessageBatchToStore batch = MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(1)
				.timestamp(timestamp)
				.content(messageContent)
				.build(), MAX_SIZE);
		
		batch.addMessage(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.timestamp(timestamp.plusSeconds(10))
				.content(messageContent)
				.build());
		
		Instant lastTimestamp = batch.getLastTimestamp();
		
		batch.addMessage(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.timestamp(timestamp.plusSeconds(20))
				.content(messageContent)
				.build());
		
		Assert.assertNotEquals(batch.getLastTimestamp(), lastTimestamp, "Last timestamp is from last added message");
	}
	
	@Test
	public void batchSerialization() throws CradleStorageException, IOException
	{
		MessageBatchToStore batch = MessageBatchToStore.singleton(builder
				.bookId(book)
				.sessionAlias(sessionAlias)
				.direction(direction)
				.sequence(0)
				.timestamp(timestamp)
				.metadata("md", "some value")
				.content(messageContent)
				.build(), MAX_SIZE);
		StoredMessage storedMsg = batch.getFirstMessage();
		byte[] bytes = MessageUtils.serializeMessages(batch.getMessages()).getSerializedData();
		StoredMessage msg = MessageUtils.deserializeMessages(bytes, batch.id).iterator().next();
		Assert.assertEquals(msg, storedMsg, "Message should be completely serialized/deserialized");
	}
	
	
	static class IdData
	{
		final BookId book;
		final String sessionAlias;
		final Direction direction;
		final Instant timestamp;
		final long sequence;
		
		public IdData(BookId book, String sessionAlias, Direction direction, Instant timestamp, long sequence)
		{
			this.book = book;
			this.sessionAlias = sessionAlias;
			this.direction = direction;
			this.timestamp = timestamp;
			this.sequence = sequence;
		}
		
		@Override
		public String toString()
		{
			return book+StoredMessageId.ID_PARTS_DELIMITER
					+sessionAlias+StoredMessageId.ID_PARTS_DELIMITER
					+direction.getLabel()+StoredMessageId.ID_PARTS_DELIMITER
					+StoredMessageIdUtils.timestampToString(timestamp)+StoredMessageId.ID_PARTS_DELIMITER
					+sequence;
		}
	}
}
