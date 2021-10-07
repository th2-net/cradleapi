/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.TestUtils;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.exactpro.cradle.messages.StoredMessageId.ID_PARTS_DELIMITER;

public class NewTest
{

	public static final String HOST = "10.64.66.101",
			DATACENTER = "srt",
			USERNAME = "kubetest",
			PASSWORD = "leuToa0S",
			SESSION_ALIAS = "TEST_Session",
			BOOK = "TEST_Book",
			PAGE = "TEST_Page", NEW_PAGE = "NEW_TEST_Page";
	public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	public static final int TIMEOUT = 50000;
	private static final BookId bookId = new BookId(BOOK);
	public static final int PORT = 9042;
	public static final Direction DIRECTION = Direction.SECOND;

	private CassandraCradleStorage storage;
	private PageInfo pageInfo;
	private long lastSequence;


	@BeforeClass
	protected void prepare() throws CradleStorageException, IOException
	{
		CassandraConnectionSettings connectionSettings = getConnectionSettings();
		CassandraStorageSettings storageSettings = getStorageSettings();
		storage = new CassandraCradleStorage(connectionSettings, storageSettings);
		storage.init(true);
		createBook();
		lastSequence = Math.max(storage.getLastSequence(SESSION_ALIAS, Direction.FIRST, bookId),
				storage.getLastSequence(SESSION_ALIAS, Direction.SECOND, bookId));
	}

	@AfterClass
	protected void dispose() throws CradleStorageException
	{
		if (storage != null)
			storage.dispose();
	}

	private void createBook() throws CradleStorageException, IOException
	{
		Optional<BookInfo> optionalBook =
				storage.getBooks().stream().filter(bookInfo -> bookInfo.getId().equals(bookId)).findAny();
		BookInfo bookInfo;
		Instant now = Instant.now();
		if (optionalBook.isPresent())
		{
			bookInfo = optionalBook.get();
		}
		else
		{
			BookToAdd bookToAdd = new BookToAdd(BOOK, now, PAGE);
			bookInfo = storage.addBook(bookToAdd);
		}
		pageInfo = bookInfo.getActivePage();
		if (pageInfo == null)
		{
			storage.switchToNewPage(bookInfo.getId(), PAGE, "TEST_Page");
			pageInfo = bookInfo.getActivePage();
		}
	}

	private CassandraStorageSettings getStorageSettings()
	{
		CassandraStorageSettings settings =
				new CassandraStorageSettings(TIMEOUT, CONSISTENCY_LEVEL, CONSISTENCY_LEVEL);
		settings.setMaxUncompressedMessageBatchSize(2000);
		settings.setMessageBatchChunkSize(300);
		return settings;
	}

	private CassandraConnectionSettings getConnectionSettings()
	{
		CassandraConnectionSettings settings = new CassandraConnectionSettings(HOST, PORT, DATACENTER);
		settings.setUsername(USERNAME);
		settings.setPassword(PASSWORD);
		return settings;
	}

	@Test
	public void storingTest() throws CradleStorageException, IOException
	{
		MessageBatchToStore batch = generateBatch(5);
		storage.storeMessageBatch(batch);
//		Collection<StoredMessage> storedMessages = storage.getMessageBatch(batch.getId());
//		for (StoredMessage storedMessage : storedMessages)
//		{
//			System.out.println(storedMessage);
//		}
	}

	@Test
	public void getSessionsTest() throws CradleStorageException, IOException
	{
		System.out.println(storage.getSessionAliases(bookId));
	}

	@Test
	public void switchPage() throws CradleStorageException, IOException
	{
		storage.switchToNewPage(bookId, NEW_PAGE, "Next page");
	}

	@Test
	public void getBatchTest() throws CradleIdException, CradleStorageException, IOException
	{
		StoredMessageId id = StoredMessageId.fromString(StringUtils.joinWith(ID_PARTS_DELIMITER, bookId,
				SESSION_ALIAS, DIRECTION.getLabel(),
				StoredMessageIdUtils.timestampToString(Instant.parse("2021-09-28T10:22:55.037052000Z")), 1));
		Collection<StoredMessage> messages = storage.getMessageBatch(id);
		if (messages != null)
		{
			for (StoredMessage message : messages)
			{
				System.out.println(message);
			}
		}
	}

	@Test
	public void getMessageTest() throws CradleIdException, CradleStorageException, IOException
	{
//		StoredMessageId id = StoredMessageId.fromString(StringUtils.joinWith(ID_PARTS_DELIMITER, bookId,
//				SESSION_ALIAS, DIRECTION.getLabel(),
//				StoredMessageIdUtils.timestampToString(Instant.parse("2021-09-28T10:22:55.037052000Z")), 3));
		StoredMessageId id = StoredMessageId.fromString("TEST_Book:TEST_Session:2:20210928102255044671000:3");
		StoredMessage message = storage.getMessage(id);
		if (message != null)
		{
			System.out.println(message);
		}
	}

	@Test
	public void getMessageAsyncTest()
			throws CradleIdException, CradleStorageException, IOException, ExecutionException, InterruptedException
	{
//		StoredMessageId id = StoredMessageId.fromString(StringUtils.joinWith(ID_PARTS_DELIMITER, bookId,
//				SESSION_ALIAS, DIRECTION.getLabel(),
//				StoredMessageIdUtils.timestampToString(Instant.parse("2021-09-28T10:22:55.037052000Z")), 3));
		StoredMessageId id = StoredMessageId.fromString("TEST_Book:TEST_Session:2:20210928102255044671000:3");
		CompletableFuture<StoredMessage> async = storage.getMessageAsync(id);
		StoredMessage storedMessage = async.get();
		if (storedMessage != null)
			System.out.println(storedMessage);
	}

	@Test
	public void getBatchesByFilter() throws CradleStorageException, IOException, CradleIdException
	{
		StoredMessageFilter filter = StoredMessageFilter.builder(bookId, SESSION_ALIAS, DIRECTION)
				.timestampFrom().isGreaterThan(Instant.now().minus(2, ChronoUnit.HOURS))
				.storedMessageId().isGreaterThanOrEqualTo(StoredMessageId.fromString("TEST_Book:TEST_Session:2:20210928135503044671000:3"))
				.build();
		System.out.println("Filter: "+filter);
		CradleResultSet<StoredMessageBatch> batches = storage.getMessagesBatches(filter);
		if (batches != null)
			for (MessageBatch batch : batches.asIterable())
			{
				for (StoredMessage message : batch.getMessages())
				{
					System.out.println(message);
				}
			}
	}

	private MessageBatchToStore generateBatch(int messagesNumber) throws CradleStorageException
	{
		MessageBatchToStore batch = new MessageBatchToStore();
		for (int i = 0; i < messagesNumber; i++)
		{
			batch.addMessage(MessageToStore.builder()
					.sequence(++lastSequence)
					.direction(DIRECTION)
					.timestamp(Instant.now())
					.bookId(bookId)
					.sessionAlias(SESSION_ALIAS)
					.content(TestUtils.createContent(3000))
					.build());
		}

		return batch;
	}

}
