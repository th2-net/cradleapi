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

package com.exactpro.cradle;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.filters.AbstractFilter;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.BookPagesNamesChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.resultset.EmptyResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Storage which holds information about all data sent or received and test events.
 */
public abstract class CradleStorage
{
	private static final Logger logger = LoggerFactory.getLogger(CradleStorage.class);
	public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;
	public static final long EMPTY_MESSAGE_INDEX = -1L;
	public static final long DEFAULT_MAX_MESSAGE_BATCH_DURATION_LIMIT_SECONDS = 600L;
	public static final int DEFAULT_MAX_MESSAGE_BATCH_SIZE = 1024*1024,
			DEFAULT_MAX_TEST_EVENT_BATCH_SIZE = DEFAULT_MAX_MESSAGE_BATCH_SIZE;

	protected BookAndPageChecker bpc;
	private volatile boolean initialized = false,
			disposed = false;
	protected final ExecutorService composingService;
	protected final boolean ownedComposingService;
	protected final CradleEntitiesFactory entitiesFactory;

	public CradleStorage(ExecutorService composingService, int maxMessageBatchSize, long maxMessageBatchDuration,
			int maxTestEventBatchSize) throws CradleStorageException
	{
		if (composingService == null)
		{
			ownedComposingService = true;
			this.composingService = Executors.newFixedThreadPool(5);
		}
		else
		{
			ownedComposingService = false;
			this.composingService = composingService;
		}
		
		entitiesFactory = new CradleEntitiesFactory(maxMessageBatchSize, maxMessageBatchDuration, maxTestEventBatchSize);
	}
	
	public CradleStorage() throws CradleStorageException
	{
		this(null, DEFAULT_MAX_MESSAGE_BATCH_SIZE, DEFAULT_MAX_MESSAGE_BATCH_DURATION_LIMIT_SECONDS,
				DEFAULT_MAX_TEST_EVENT_BATCH_SIZE);
	}
	

	protected abstract void doInit(boolean prepareStorage) throws CradleStorageException;
	protected abstract BookCache getBookCache ();
	protected abstract void doDispose() throws CradleStorageException;

	protected abstract Collection<BookListEntry> doListBooks ();
	protected abstract void doAddBook(BookToAdd newBook, BookId bookId) throws IOException;
	protected abstract void doAddPages(BookId bookId, List<PageInfo> pages, PageInfo lastPage) throws CradleStorageException, IOException;
	protected abstract Collection<PageInfo> doLoadPages(BookId bookId) throws CradleStorageException, IOException;
	protected abstract Collection<PageInfo> doGetAllPages(BookId bookId) throws CradleStorageException;
	protected abstract void doRemovePage(PageInfo page) throws CradleStorageException, IOException;
	
	
	protected abstract void doStoreMessageBatch(MessageBatchToStore batch, PageInfo page) throws IOException, CradleStorageException;
	protected abstract void doStoreGroupedMessageBatch(MessageBatchToStore batch, PageInfo page, String groupName)
			throws IOException;
	protected abstract CompletableFuture<Void> doStoreMessageBatchAsync(MessageBatchToStore batch, PageInfo page) 
			throws IOException, CradleStorageException;
	protected abstract CompletableFuture<Void> doStoreGroupedMessageBatchAsync(MessageBatchToStore batch, PageInfo page,
			String groupName) throws IOException, CradleStorageException;
	
	
	protected abstract void doStoreTestEvent(TestEventToStore event, PageInfo page) throws IOException, CradleStorageException;
	protected abstract CompletableFuture<Void> doStoreTestEventAsync(TestEventToStore event, PageInfo page) throws IOException, CradleStorageException;
	protected abstract void doUpdateParentTestEvents(TestEventToStore event) throws IOException;
	protected abstract CompletableFuture<Void> doUpdateParentTestEventsAsync(TestEventToStore event);
	protected abstract void doUpdateEventStatus(StoredTestEvent event, boolean success) throws IOException;
	protected abstract CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEvent event, boolean success);
	
	
	protected abstract StoredMessage doGetMessage(StoredMessageId id, PageId pageId) throws IOException, CradleStorageException;
	protected abstract CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id, PageId pageId)
			throws CradleStorageException;
	protected abstract StoredMessageBatch doGetMessageBatch(StoredMessageId id, PageId pageId) throws IOException, CradleStorageException;
	protected abstract CompletableFuture<StoredMessageBatch> doGetMessageBatchAsync(StoredMessageId id, PageId pageId)
			throws CradleStorageException;
	
	protected abstract CradleResultSet<StoredMessage> doGetMessages(MessageFilter filter, BookInfo book) 
			throws IOException, CradleStorageException;
	protected abstract CompletableFuture<CradleResultSet<StoredMessage>> doGetMessagesAsync(MessageFilter filter, BookInfo book)
			throws CradleStorageException;
	protected abstract CradleResultSet<StoredMessageBatch> doGetMessageBatches(MessageFilter filter, BookInfo book) 
			throws IOException, CradleStorageException;
	protected abstract CradleResultSet<StoredMessageBatch> doGetGroupedMessageBatches(GroupedMessageFilter filter, BookInfo book) 
			throws IOException, CradleStorageException;
	protected abstract CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetMessageBatchesAsync(MessageFilter filter,
			BookInfo book) throws CradleStorageException;
	protected abstract CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetGroupedMessageBatchesAsync(GroupedMessageFilter filter,
			BookInfo book) throws CradleStorageException;
	
	protected abstract long doGetLastSequence(String sessionAlias, Direction direction, BookId bookId)
			throws IOException, CradleStorageException;
	protected abstract long doGetFirstSequence(String sessionAlias, Direction direction, BookId bookId)
			throws IOException, CradleStorageException;
	protected abstract Collection<String> doGetSessionAliases(BookId bookId) throws IOException, CradleStorageException;
	
	
	protected abstract StoredTestEvent doGetTestEvent(StoredTestEventId id, PageId pageId) throws IOException, CradleStorageException;
	protected abstract CompletableFuture<StoredTestEvent> doGetTestEventAsync(StoredTestEventId ids, PageId pageId) throws CradleStorageException;
	protected abstract CradleResultSet<StoredTestEvent> doGetTestEvents(TestEventFilter filter, BookInfo book) 
			throws IOException, CradleStorageException;
	protected abstract CompletableFuture<CradleResultSet<StoredTestEvent>> doGetTestEventsAsync(TestEventFilter filter, BookInfo book) 
			throws CradleStorageException;
	
	protected abstract Collection<String> doGetScopes(BookId bookId) throws IOException, CradleStorageException;

	protected abstract CompletableFuture<CradleResultSet<CounterSample>> doGetMessageCountersAsync(BookId bookId,
																								   String sessionAlias,
																								   Direction direction,
																								   FrameType frameType,
																								   Interval interval) throws CradleStorageException;
	protected abstract CradleResultSet<CounterSample> doGetMessageCounters(BookId bookId,
																		   String sessionAlias,
																		   Direction direction,
																		   FrameType frameType,
																		   Interval interval) throws CradleStorageException, IOException;

	protected abstract CompletableFuture<CradleResultSet<CounterSample>> doGetCountersAsync(BookId bookId,
																							EntityType entityType,
																							FrameType frameType,
																							Interval interval) throws CradleStorageException;
	protected abstract CradleResultSet<CounterSample> doGetCounters(BookId bookId,
																	EntityType entityType,
																	FrameType frameType,
																	Interval interval) throws CradleStorageException, IOException;


	protected abstract CompletableFuture<Counter> doGetMessageCountAsync(BookId bookId,
																		 String sessionAlias,
																		 Direction direction,
																		 Interval interval) throws CradleStorageException;

	protected abstract Counter doGetMessageCount(BookId bookId,
												 String sessionAlias,
												 Direction direction,
												 Interval interval) throws CradleStorageException, IOException;

	protected abstract CompletableFuture<Counter> doGetCountAsync (BookId bookId,
																   EntityType entityType,
																   Interval interval) throws CradleStorageException;

	protected abstract Counter doGetCount (BookId bookId,
										   EntityType entityType,
										   Interval interval) throws CradleStorageException, IOException;

	protected abstract PageInfo doUpdatePageComment (BookId bookId, String pageName, String comment) throws CradleStorageException;

	protected abstract PageInfo doUpdatePageName (BookId bookId, String pageName, String newPageName) throws CradleStorageException;



	/**
	 * Initializes internal objects of storage and prepares it to access data, i.e. creates needed connections and facilities.
	 * @param prepareStorage if underlying physical storage should be created, if absent
	 * @throws CradleStorageException if storage initialization failed
	 * @throws IOException if data reading or creation of storage failed
	 */
	public void init(boolean prepareStorage) throws CradleStorageException, IOException
	{
		if (initialized)
			return;
		
		logger.info("Initializing storage");
		
		doInit(prepareStorage);
		bpc = new BookAndPageChecker(getBookCache());
		initialized = true;
		logger.info("Storage initialized");
	}
	
	/**
	 * IntervalsWorker is used to work with Crawler intervals
	 * @param pageId page to get worker for
	 * @return instance of IntervalsWorker
	 */
	public abstract IntervalsWorker getIntervalsWorker(PageId pageId);
	
	
	/**
	 * Disposes resources occupied by storage which means closing of opened connections, flushing all buffers, etc.
	 * @throws CradleStorageException if there was error during storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	public final void dispose() throws CradleStorageException
	{
		if (disposed)
			return;
		
		logger.info("Disposing storage");
		
		if (ownedComposingService)
		{
			logger.info("Shutting down composing service...");
			composingService.shutdownNow();
		}
		
		doDispose();
		disposed = true;
		logger.info("Storage disposed");
	}
	
	/**
	 * @return true if storage is already disposed and false if it is not disposed, including the case when disposal failed with error
	 */
	public final boolean isDisposed()
	{
		return disposed;
	}
	
	
	/**
	 * Creates new book and adds it to storage, adding page with given name to newly created book
	 * @param book information about book to add and its first page
	 * @return {@link BookInfo} containing all information about created book
	 * @throws CradleStorageException if the book is already present
	 * @throws IOException if book data writing failed
	 */
	public BookInfo addBook(BookToAdd book) throws CradleStorageException, IOException
	{
		BookPagesNamesChecker.validateBookName(book.getName());
		BookPagesNamesChecker.validatePageName(book.getFirstPageName());

		BookId id = new BookId(book.getName());
		logger.info("Adding book '{}' to storage", id);
		if (bpc.checkBook(id))
			throw new CradleStorageException("Book '"+id+"' is already present in storage");
		
		doAddBook(book, id);
		BookInfo newBook = new BookInfo(id, book.getFullName(), book.getDesc(), book.getCreated(), null);
		getBookCache().updateCachedBook(newBook);
		logger.info("Book '{}' has been added to storage", id);
		
		newBook = addPage(id, book.getFirstPageName(), book.getCreated(), book.getFirstPageComment());
		getBookCache().updateCachedBook(newBook);

		return newBook;
	}

	/**
	 * Gets books listed in underlying DB, does not validate them
	 * or add to cache
	 * @return Collection of BookListEntry which contains minimal information about books
	 */
	public Collection<BookListEntry> listBooks () {
		return doListBooks();
	}

	/**
	 * @return collection of books currently available in storage
	 */
	public Collection<BookInfo> getBooks()
	{
		return Collections.unmodifiableCollection(getBookCache().getCachedBooks());
	}
	
	/**
	 * Adds to given book the new page, started at current timestamp. 
	 * Last page of the book will be marked as ended at timestamp of new page start
	 * @param bookId ID of the book where to add the page
	 * @param pageName name of new page
	 * @param pageStart timestamp of new page start
	 * @param pageComment optional comment for new page
	 * @return updated book information
	 * @throws CradleStorageException if given bookId is unknown or page with given name already exists in this book
	 * @throws IOException if page data writing failed
	 */
	public BookInfo addPage(BookId bookId, String pageName, Instant pageStart, String pageComment) throws CradleStorageException, IOException
	{
		return addPages(bookId, Collections.singletonList(new PageToAdd(pageName, pageStart, pageComment)));
	}
	
	/**
	 * Adds new pages to given book. 
	 * Last page of the book will be marked as ended at start timestamp of the first page being added
	 * @param bookId ID of the book where to add the page
	 * @param pages to add
	 * @return updated book information
	 * @throws CradleStorageException if given bookId is unknown, page to add already exists or new pages are not in ascending order
	 * @throws IOException if page data writing failed
	 */
	public BookInfo addPages(BookId bookId, List<PageToAdd> pages) throws CradleStorageException, IOException
	{
		logger.info("Adding pages {} to book '{}'", pages, bookId);
		
		BookInfo book = refreshPages(bookId);
		if (pages == null || pages.isEmpty())
			return book;
		
		List<PageInfo> toAdd = checkPages(pages, book);
		
		PageInfo lastPage = book.getLastPage();
		PageInfo endedPage;
		if (lastPage != null && lastPage.getEnded() == null)
			endedPage = PageInfo.ended(lastPage, toAdd.get(0).getStarted());
		else
			endedPage = null;
		
		try
		{
			doAddPages(bookId, toAdd, endedPage);
		}
		catch (IOException e)
		{
			//Need to refresh book's pages to make user able to see what was the reason of failure, e.g. new page was actually present
			refreshPages(bookId);
			throw e;
		}
		
		if (endedPage != null)
			book.addPage(endedPage);  //Replacing last page with ended one, i.e. updating last page info
		for (PageInfo newPage : toAdd)
			book.addPage(newPage);
		
		return book;
	}
	
	/**
	 * Refreshes pages information of given book, loading actual data from storage. 
	 * Use this method to refresh Cradle API internal book cache when new pages were added to the book or removed outside of the application
	 * @param bookId ID of the book whose pages to refresh
	 * @return refreshed book information
	 * @throws CradleStorageException if given bookId is unknown
	 * @throws IOException if page data reading failed
	 */
	public BookInfo refreshPages(BookId bookId) throws CradleStorageException, IOException
	{
		logger.info("Refreshing pages of book '{}'", bookId);
		BookInfo book = bpc.getBook(bookId);
		Collection<PageInfo> pages = doLoadPages(bookId);
		book = new BookInfo(book.getId(), book.getFullName(), book.getDesc(), book.getCreated(), pages);
		getBookCache().updateCachedBook(book);
		return book;
	}

	/**
	 * @param bookId book of removed pages
	 * @return collection of removed pages for given book
	 * @throws CradleStorageException  If there was problem loading pages
	 */
	public Collection<PageInfo> getAllPages(BookId bookId) throws CradleStorageException {
		logger.info("Getting Removed pages for book {}", bookId.getName());

		try {
			return doGetAllPages(bookId);
		} catch (CradleStorageException e) {
			logger.error("Could not get removed pages for book {}", bookId.getName());
			throw e;
		}
	}

	/**
	 * Getting information about specific book from storage and put it in internal cache
	 * @param name of book to load
	 * @return loaded book
	 * @throws CradleStorageException if book data reading failed
	 */
	public BookInfo refreshBook (String name) throws CradleStorageException {
		logger.info("Refreshing book {} from storage", name);

		BookInfo bookInfo = getBookCache().loadBook(new BookId(name));

		if (bookInfo != null) {
			getBookCache().updateCachedBook(bookInfo);
		}

		return bookInfo;
	}
	
	/**
	 * Removes page with given ID, deleting all messages and test events stored within that page
	 * @param pageId ID of page to remove
	 * @return refreshed book information
	 * @throws CradleStorageException if given page ID or its book is unknown or the page is currently the active one
	 * @throws IOException if page data removal failed
	 */
	public BookInfo removePage(PageId pageId) throws CradleStorageException, IOException
	{
		logger.info("Removing page '{}'", pageId);
		
		BookId bookId = pageId.getBookId();
		BookInfo book = refreshPages(bookId);
		
		String pageName = pageId.getName();
		PageInfo page = book.getPage(pageId);
		if (page == null)
			throw new CradleStorageException("Page '"+pageName+"' is not present in book '"+bookId+"'");
		doRemovePage(page);
		book.removePage(pageId);
		logger.info("Page '{}' has been removed", pageId);
		return book;
	}
	
	
	/**
	 * @return factory to create message and test event batches that conform with storage settings
	 */
	public CradleEntitiesFactory getEntitiesFactory()
	{
		return entitiesFactory;
	}
	
	
	/**
	 * Writes data about given message batch to current page
	 * @param batch data to write
	 * @throws IOException if data writing failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final void storeMessageBatch(MessageBatchToStore batch) throws IOException, CradleStorageException
	{
		StoredMessageId id = batch.getId();
		logger.debug("Storing message batch {}", id);
		PageInfo page = bpc.findPage(id.getBookId(), id.getTimestamp());
		doStoreMessageBatch(batch, page);
		logger.debug("Message batch {} has been stored", id);
	}

	
	public final void storeGroupedMessageBatch(MessageBatchToStore batch, String groupName)
			throws CradleStorageException, IOException
	{
		StoredMessageId id = batch.getId();
		logger.debug("Storing message batch {} grouped by {}", id, groupName);
		PageInfo page = bpc.findPage(id.getBookId(), id.getTimestamp());
		doStoreGroupedMessageBatch(batch, page, groupName);
		logger.debug("Message batch {} grouped by {} has been stored", id, groupName);
	}
	
	
	/**
	 * Asynchronously writes data about given message batch to current page
	 * @param batch data to write
	 * @return future to get know if storing was successful
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data writing failed
	 */
	public final CompletableFuture<Void> storeMessageBatchAsync(MessageBatchToStore batch)
			throws CradleStorageException, IOException
	{
		StoredMessageId id = batch.getId();
		logger.debug("Storing message batch {} asynchronously", id);
		PageInfo page = bpc.findPage(id.getBookId(), id.getTimestamp());
		CompletableFuture<Void> result = doStoreMessageBatchAsync(batch, page);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while storing message batch "+id+" asynchronously", error);
				else
					logger.debug("Message batch {} has been stored asynchronously", id);
			}, composingService);
		return result;
	}

	/**
	 * Asynchronously writes data about given message batch to current page grouped by provided group name
	 * @param batch data to write
	 * @param groupName group name
	 * @return future to get know if storing was successful
	 * @throws CradleStorageException if given parameters are invalid
	 * @throws IOException if data writing failed
	 */
	public final CompletableFuture<Void> storeGroupedMessageBatchAsync(MessageBatchToStore batch, String groupName)
			throws CradleStorageException, IOException
	{
		if (groupName == null)
			throw new CradleStorageException("'groupName' is required parameter and can not be null");
		
		StoredMessageId id = batch.getId();
		logger.debug("Storing message batch {} grouped by {} asynchronously", id, groupName);
		PageInfo page = bpc.findPage(id.getBookId(), id.getTimestamp());
		CompletableFuture<Void> result = doStoreGroupedMessageBatchAsync(batch, page, groupName);
		result.whenCompleteAsync((r, error) -> {
			if (error != null)
				logger.error("Error while storing message batch "+id+" grouped by "+groupName+" asynchronously", error);
			else
				logger.debug("Message batch {} grouped by {} has been stored asynchronously", id, groupName);
		}, composingService);
		return result;
	}


	/**
	 * Writes data about given test event to current page
	 * @param event data to write
	 * @throws IOException if data writing failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final void storeTestEvent(TestEventToStore event) throws IOException, CradleStorageException
	{
		StoredTestEventId id = event.getId();
		logger.debug("Storing test event {}", id);
		PageInfo page = bpc.findPage(id.getBookId(), id.getStartTimestamp());
		
		TestEventUtils.validateTestEvent(event);
		
		doStoreTestEvent(event, page);
		logger.debug("Test event {} has been stored", id);
		if (event.getParentId() != null)
		{
			logger.debug("Updating parents of test event {}", id);
			doUpdateParentTestEvents(event);
			logger.debug("Parents of test event {} have been updated", id);
		}
	}
	
	/**
	 * Asynchronously writes data about given test event to current page
	 * @param event data to write
	 * @throws IOException if data is invalid
	 * @return future to get know if storing was successful
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Void> storeTestEventAsync(TestEventToStore event) throws IOException, CradleStorageException
	{
		StoredTestEventId id = event.getId();
		logger.debug("Storing test event {} asynchronously", id);
		PageInfo page = bpc.findPage(id.getBookId(), id.getStartTimestamp());
		
		TestEventUtils.validateTestEvent(event);
		
		CompletableFuture<Void> result = doStoreTestEventAsync(event, page);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while storing test event "+id+" asynchronously", error);
				else
					logger.debug("Test event {} has been stored asynchronously", id);
			}, composingService);
		
		if (event.getParentId() == null)
			return result;
		
		return result.thenComposeAsync(r -> {
			logger.debug("Updating parents of test event {} asynchronously", id);
			CompletableFuture<Void> result2 = doUpdateParentTestEventsAsync(event);
			result2.whenCompleteAsync((r2, error) -> {
					if (error != null)
						logger.error("Error while updating parents of test event "+id+" asynchronously", error);
					else
						logger.debug("Parents of test event {} have been updated asynchronously", event.getId());
				}, composingService);
			return result2;
		}, composingService);
	}
	
	
	/**
	 * Retrieves message data stored under given ID
	 * @param id of stored message to retrieve
	 * @return data of stored message
	 * @throws IOException if message data retrieval failed
	 * @throws CradleStorageException if given parameter is invalid
	 */
	public final StoredMessage getMessage(StoredMessageId id) throws IOException, CradleStorageException
	{
		logger.debug("Getting message {}", id);
		PageId pageId = bpc.findPage(id.getBookId(), id.getTimestamp()).getId();
		StoredMessage result = doGetMessage(id, pageId);
		logger.debug("Message {} got from page {}", id, pageId);
		return result;
	}
	
	/**
	 * Asynchronously retrieves message data stored under given ID
	 * @param id of stored message to retrieve
	 * @return future to obtain data of stored message
	 * @throws CradleStorageException if given parameter is invalid
	 */
	public final CompletableFuture<StoredMessage> getMessageAsync(StoredMessageId id) throws CradleStorageException
	{
		logger.debug("Getting message {} asynchronously", id);
		PageId pageId = bpc.findPage(id.getBookId(), id.getTimestamp()).getId();
		CompletableFuture<StoredMessage> result = doGetMessageAsync(id, pageId);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while getting message "+id+" from page "+pageId+" asynchronously", error);
				else
					logger.debug("Message {} from page {} got asynchronously", id, pageId);
			}, composingService);
		return result;
	}
	
	
	/**
	 * Retrieves the batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @return batch of messages
	 * @throws IOException if batch data retrieval failed
	 * @throws CradleStorageException if given parameter is invalid
	 */
	public final StoredMessageBatch getMessageBatch(StoredMessageId id) throws IOException, CradleStorageException
	{
		logger.debug("Getting message batch by message ID {}", id);
		PageId pageId = bpc.findPage(id.getBookId(), id.getTimestamp()).getId();
		StoredMessageBatch result = doGetMessageBatch(id, pageId);
		logger.debug("Message batch by message ID {} got from page {}", id, pageId);
		return result;
	}
	
	/**
	 * Asynchronously retrieves the batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @return future to obtain batch of messages
	 * @throws CradleStorageException if given parameter is invalid
	 */
	protected final CompletableFuture<StoredMessageBatch> getMessageBatchAsync(StoredMessageId id) throws CradleStorageException
	{
		logger.debug("Getting message batch by message ID {} asynchronously", id);
		PageId pageId = bpc.findPage(id.getBookId(), id.getTimestamp()).getId();
		CompletableFuture<StoredMessageBatch> result = doGetMessageBatchAsync(id, pageId);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while getting message batch by message ID "+id+" from page "+pageId+" asynchronously", error);
				else
					logger.debug("Message batch by message ID {} from page {} got asynchronously", id, pageId);
			}, composingService);
		return result;
	}
	
	/**
	 * Allows enumerating stored messages filtering them by given conditions
	 * @param filter defines conditions to filter messages by
	 * @return result set to enumerate messages
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if filter is invalid
	 */
	public final CradleResultSet<StoredMessage> getMessages(MessageFilter filter) throws IOException, CradleStorageException
	{
		logger.debug("Filtering messages by {}", filter);
		if (!checkFilter(filter))
			return new EmptyResultSet<>();
		
		BookInfo book = bpc.getBook(filter.getBookId());
		CradleResultSet<StoredMessage> result = doGetMessages(filter, book);
		logger.debug("Got result set with messages filtered by {}", filter);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain result set to enumerate stored messages filtering them by given conditions
	 * @param filter defines conditions to filter messages by
	 * @return future to obtain result set to enumerate messages
	 * @throws CradleStorageException if filter is invalid
	 */
	public final CompletableFuture<CradleResultSet<StoredMessage>> getMessagesAsync(MessageFilter filter) throws CradleStorageException
	{
		logger.debug("Asynchronously getting messages filtered by {}", filter);
		if (!checkFilter(filter))
			return CompletableFuture.completedFuture(new EmptyResultSet<>());
		
		BookInfo book = bpc.getBook(filter.getBookId());
		CompletableFuture<CradleResultSet<StoredMessage>> result = doGetMessagesAsync(filter, book);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while getting messages filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Result set with messages filtered by {} got asynchronously", filter);
			}, composingService);
		return result;
	}
	
	
	/**
	 * Allows enumerating stored message batches filtering them by given conditions
	 * @param filter defines conditions to filter message batches by
	 * @return result set to enumerate message batches
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if filter is invalid
	 */
	public final CradleResultSet<StoredMessageBatch> getMessageBatches(MessageFilter filter) throws IOException, CradleStorageException
	{
		logger.debug("Filtering message batches by {}", filter);
		if (!checkFilter(filter))
			return new EmptyResultSet<>();
		
		BookInfo book = bpc.getBook(filter.getBookId());
		CradleResultSet<StoredMessageBatch> result = doGetMessageBatches(filter, book);
		logger.debug("Got result set with message batches filtered by {}", filter);
		return result;
	}


	/**
	 * Allows enumerating stored message batches filtering them by given conditions
	 * @param filter defines conditions to filter message batches by
	 * @return result set to enumerate message batches
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if filter is invalid
	 */
	public final CradleResultSet<StoredMessageBatch> getGroupedMessageBatches(GroupedMessageFilter filter)
			throws CradleStorageException, IOException
	{
		logger.debug("Filtering grouped message batches by {}", filter);
		checkAbstractFilter(filter);

		BookInfo book = bpc.getBook(filter.getBookId());
		CradleResultSet<StoredMessageBatch> result = doGetGroupedMessageBatches(filter, book);
		logger.debug("Got result set with grouped message batches filtered by {}", filter);
		return result;
	}
	
	
	/**
	 * Allows to asynchronously obtain result set to enumerate stored message batches filtering them by given conditions
	 * @param filter defines conditions to filter message batches by
	 * @return future to obtain result set to enumerate message batches
	 * @throws CradleStorageException if filter is invalid
	 */
	public final CompletableFuture<CradleResultSet<StoredMessageBatch>> getMessageBatchesAsync(MessageFilter filter) throws CradleStorageException
	{
		logger.debug("Asynchronously getting message batches filtered by {}", filter);
		if (!checkFilter(filter))
			return CompletableFuture.completedFuture(new EmptyResultSet<>());
		
		BookInfo book = bpc.getBook(filter.getBookId());
		CompletableFuture<CradleResultSet<StoredMessageBatch>> result = doGetMessageBatchesAsync(filter, book);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while getting message batches filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Result set with message batches filtered by {} got asynchronously", filter);
			}, composingService);
		return result;
	}
	
	
	/**
	 * Allows to asynchronously obtain result set to enumerate stored message batches filtering them by given conditions
	 * @param filter defines conditions to filter message batches by
	 * @return future to obtain result set to enumerate message batches
	 * @throws CradleStorageException if filter is invalid
	 */
	public final CompletableFuture<CradleResultSet<StoredMessageBatch>> getGroupedMessageBatchesAsync(GroupedMessageFilter filter) throws CradleStorageException
	{
		logger.debug("Asynchronously getting grouped message batches filtered by {}", filter);
		checkAbstractFilter(filter);
		
		BookInfo book = bpc.getBook(filter.getBookId());
		CompletableFuture<CradleResultSet<StoredMessageBatch>> result = doGetGroupedMessageBatchesAsync(filter, book);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while getting message batches filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Result set with message batches filtered by {} got asynchronously", filter);
			}, composingService);
		return result;
	}
	
	
	/**
	 * Retrieves last stored sequence number for given session alias and direction within given page. 
	 * Use result of this method to continue writing messages.
	 * @param sessionAlias to get sequence number for 
	 * @param direction to get sequence number for
	 * @param bookId to get last sequence for
	 * @return last stored sequence number for given arguments, if it is present, -1 otherwise
	 * @throws IOException if retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final long getLastSequence(String sessionAlias, Direction direction, BookId bookId) throws IOException, CradleStorageException
	{
		logger.debug("Getting last stored sequence number for book '{}' and session alias '{}' and direction '{}'",
				bookId, sessionAlias, direction.getLabel());
		long result = doGetLastSequence(sessionAlias, direction, bookId);
		logger.debug("Sequence number {} got", result);
		return result;
	}


	/**
	 * Retrieves first stored sequence number for given session alias and direction within given page.
	 * @param sessionAlias to get sequence number for
	 * @param direction to get sequence number for
	 * @param bookId to get last sequence for
	 * @return first stored sequence number for given arguments, if it is present, -1 otherwise
	 * @throws IOException if retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final long getFirstSequence(String sessionAlias, Direction direction, BookId bookId) throws IOException, CradleStorageException
	{
		logger.debug("Getting first stored sequence number for book '{}' and session alias '{}' and direction '{}'",
				bookId, sessionAlias, direction.getLabel());
		long result = doGetFirstSequence(sessionAlias, direction, bookId);
		logger.debug("Sequence number {} got", result);
		return result;
	}



	/**
	 * Obtains collection of session aliases whose messages are saved in given book
	 * @param bookId to get session aliases from
	 * @return collection of session aliases
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if given book ID is invalid
	 */
	public final Collection<String> getSessionAliases(BookId bookId) throws IOException, CradleStorageException
	{
		logger.debug("Getting session aliases for book '{}'", bookId);
		bpc.getBook(bookId);
		Collection<String> result = doGetSessionAliases(bookId);
		logger.debug("Session aliases for book '{}' got", bookId);
		return result;
	}
	
	
	/**
	 * Retrieves test event data stored under given ID
	 * @param id of stored test event to retrieve
	 * @return data of stored test event
	 * @throws IOException if test event data retrieval failed
	 * @throws CradleStorageException if given parameter is invalid
	 */
	public final StoredTestEvent getTestEvent(StoredTestEventId id) throws IOException, CradleStorageException
	{
		logger.debug("Getting test event {}", id);
		PageId pageId = bpc.findPage(id.getBookId(), id.getStartTimestamp()).getId();
		StoredTestEvent result = doGetTestEvent(id, pageId);
		logger.debug("Test event {} got from page {}", id, pageId);
		return result;
	}
	
	/**
	 * Asynchronously retrieves test event data stored under given ID
	 * @param id of stored test event to retrieve
	 * @return future to obtain data of stored test event
	 * @throws CradleStorageException if given parameter is invalid
	 */
	public final CompletableFuture<StoredTestEvent> getTestEventAsync(StoredTestEventId id) throws CradleStorageException
	{
		logger.debug("Getting test event {} asynchronously", id);
		PageId pageId = bpc.findPage(id.getBookId(), id.getStartTimestamp()).getId();
		CompletableFuture<StoredTestEvent> result = doGetTestEventAsync(id, pageId);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while getting test event "+id+" from page "+pageId+" asynchronously", error);
				else
					logger.debug("Test event {} from page {} got asynchronously", id, pageId);
			}, composingService);
		return result;
	}
	
	
	/**
	 * Allows to enumerate test events, filtering them by given conditions
	 * @param filter defines conditions to filter test events by
	 * @return result set to enumerate test events
	 * @throws CradleStorageException if filter is invalid
	 * @throws IOException if data retrieval failed
	 */
	public final CradleResultSet<StoredTestEvent> getTestEvents(TestEventFilter filter) throws CradleStorageException, IOException
	{
		logger.debug("Filtering test events by {}", filter);
		if (!checkFilter(filter))
			return new EmptyResultSet<>();
		
		BookInfo book = bpc.getBook(filter.getBookId());
		CradleResultSet<StoredTestEvent> result = doGetTestEvents(filter, book);
		logger.debug("Got result set with test events filtered by {}", filter);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain result set to enumerate test events, filtering them by given conditions
	 * @param filter defines conditions to filter test events by
	 * @return future to obtain result set to enumerate test events
	 * @throws CradleStorageException if filter is invalid
	 * @throws IOException if data retrieval failed
	 */
	public final CompletableFuture<CradleResultSet<StoredTestEvent>> getTestEventsAsync(TestEventFilter filter) throws CradleStorageException, IOException
	{
		logger.debug("Asynchronously getting test events filtered by {}", filter);
		if (!checkFilter(filter))
			return CompletableFuture.completedFuture(new EmptyResultSet<>());
		
		BookInfo book = bpc.getBook(filter.getBookId());
		CompletableFuture<CradleResultSet<StoredTestEvent>> result = doGetTestEventsAsync(filter, book);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while getting test events filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Result set with test events filtered by {} got asynchronously", filter);
			}, composingService);
		return result;
	}
	
	/**
	 * Obtains collection of scope names whose test events are saved in given book
	 * @param bookId to get scopes from
	 * @return collection of scope names
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if given book ID is invalid
	 */
	public final Collection<String> getScopes(BookId bookId) throws IOException, CradleStorageException
	{
		logger.debug("Getting scopes for book '{}'", bookId);
		bpc.getBook(bookId);
		Collection<String> result = doGetScopes(bookId);
		logger.debug("Scopes for book '{}' got", bookId);
		return result;
	}

	/**
	 * Gets counters for message densities for specified granularity and time frame asynchronously
	 * @param bookId identifier for book
	 * @param sessionAlias session alias
	 * @param direction direction
	 * @param frameType frame type
	 * @param interval time interval
	 * @return returns CounterSamples for message densities
	 * @throws CradleStorageException if given book ID is invalid
	 */
	public CompletableFuture<CradleResultSet<CounterSample>> getMessageCountersAsync(BookId bookId,
																			   String sessionAlias,
																			   Direction direction,
																			   FrameType frameType,
																			   Interval interval) throws CradleStorageException {
		return doGetMessageCountersAsync(bookId,  sessionAlias, direction, frameType, interval);
	}

	/**
	 * Gets counters for message densities for specified granularity and time frame
	 * @param bookId identifier for book
	 * @param sessionAlias session alias
	 * @param direction direction
	 * @param frameType frameType
	 * @param interval time interval
	 * @return returns CounterSamples for message densities
	 * @throws CradleStorageException if given book ID is invalid
	 * @throws IOException if there is a problem with input/output
	 */
	public CradleResultSet<CounterSample> getMessageCounters(BookId bookId,
													   String sessionAlias,
													   Direction direction,
													   FrameType frameType,
													   Interval interval) throws CradleStorageException, IOException {
		return doGetMessageCounters(bookId, sessionAlias, direction, frameType, interval);
	}

	/**
	 * Gets counters for entity densities for specified granularity and time frame asynchronously
	 * @param bookId identifier for book
	 * @param entityType entity type
	 * @param frameType frameType
	 * @param interval time interval
	 * @return returns CounterSamples for entity densities
	 * @throws CradleStorageException if given book ID is invalid
	 */
	public CompletableFuture<CradleResultSet<CounterSample>> getCountersAsync (BookId bookId,
																	EntityType entityType,
																	FrameType frameType,
																	Interval interval) throws CradleStorageException {
		return doGetCountersAsync(bookId, entityType, frameType, interval);
	}

	/**
	 * Gets counters for entity densities for specified granularity and time frame
	 * @param bookId identifier for book
	 * @param entityType entity type
	 * @param frameType frameType
	 * @param interval time interval
	 * @return returns CounterSamples for entity densities
	 * @throws CradleStorageException if given book ID is invalid
	 * @throws IOException if there is a problem with input/output
	 */
	public CradleResultSet<CounterSample> getCounters (BookId bookId,
													   EntityType entityType,
													   FrameType frameType,
													   Interval interval) throws CradleStorageException, IOException {
		return doGetCounters(bookId, entityType, frameType, interval);
	}


	/**
	 * Gets accumulated counter for given interval asynchronously
	 * @param bookId identifier for book
	 * @param entityType entity type
	 * @param interval time interval
	 * @return returns Counter for given interval
	 * @throws CradleStorageException if given book ID is invalid
	 */
	public CompletableFuture<Counter> getCountAsync (BookId bookId,
													 EntityType entityType,
													 Interval interval) throws CradleStorageException {
		return doGetCountAsync(bookId, entityType, interval);
	}

	/**
	 * Gets accumulated counter for messages with
	 * given fields and interval asynchronously
	 * @param bookId identifier for book
	 * @param sessionAlias session alias
	 * @param direction direction
	 * @param interval time interval
	 * @return returns Counter for given messages
	 * @throws CradleStorageException if given book ID is invalid
	 */
	public CompletableFuture<Counter> getMessageCountAsync (BookId bookId,
															String sessionAlias,
															Direction direction,
															Interval interval) throws CradleStorageException {
		return doGetMessageCountAsync(bookId, sessionAlias, direction, interval);
	}

	/**
	 * Gets accumulated counter for given interval
	 * @param bookId identifier for book
	 * @param entityType entity type
	 * @param interval time interval
	 * @return returns Counter for given interval
	 * @throws CradleStorageException if given book ID is invalid
	 * @throws IOException if there is a problem with input/output
	 */
	public Counter getCount (BookId bookId,
							 EntityType entityType,
							 Interval interval) throws CradleStorageException, IOException {
		return doGetCount(bookId, entityType, interval);
	}

	/**
	 * Gets accumulated counter for messages with
	 * given fields and interval
	 * @param bookId identifier for book
	 * @param sessionAlias session alias
	 * @param direction direction
	 * @param interval time interval
	 * @return returns Counter for given messages
	 * @throws CradleStorageException if given book ID is invalid
	 * @throws IOException if there is a problem with input/output
	 */
	public Counter getMessageCount (BookId bookId,
									String page,
									String sessionAlias,
									Direction direction,
									Interval interval) throws CradleStorageException, IOException {

		return doGetMessageCount(bookId, sessionAlias, direction, interval);
	}

	/**
	 *	Updates comment field for page
	 * @param bookId Identifier for book
	 * @param pageName name of page to update
	 * @param comment updated comment value for page
	 * @return returns PageInfo of updated page
	 * @throws CradleStorageException Page was edited but cache wasn't refreshed, try to refresh pages
	 * @throws IOException if there is a problem with input/output
	 */
	public PageInfo updatePageComment (BookId bookId, String pageName, String comment) throws CradleStorageException, IOException {
		bpc.getBook(bookId);
		PageInfo updatedPageInfo = doUpdatePageComment(bookId, pageName, comment);

		try {
			updatePage(new PageId(bookId, pageName), updatedPageInfo);
		} catch (Exception e) {
			logger.error("Page was edited but cache wasn't refreshed, try to refresh pages");
			throw  e;
		}

		return updatedPageInfo;
	}

	/**
	 *	Updates page name
	 * @param bookId Identifier for book
	 * @param pageName name of page to update
	 * @param newPageName name after update
	 * @return returns PageInfo of updated page
	 * @throws CradleStorageException Page was edited but cache wasn't refreshed, try to refresh pages
	 * @throws IOException if there is a problem with input/output
	 */
	public PageInfo updatePageName (BookId bookId, String pageName, String newPageName) throws CradleStorageException, IOException {
		bpc.getBook(bookId);
		PageInfo updatedPageInfo = doUpdatePageName(bookId, pageName, newPageName);

		try {
			updatePage(new PageId(bookId, pageName), updatedPageInfo);
		} catch (Exception e) {
			logger.error("Page was edited but cache wasn't refreshed, try to refresh pages");
			throw  e;
		}

		return updatedPageInfo;
	}

	private void updatePage(PageId pageId, PageInfo updatedPageInfo) throws CradleStorageException {
		BookInfo bookInfo = bpc.getBook(pageId.getBookId());

		bookInfo.removePage(pageId);
		bookInfo.addPage(updatedPageInfo);
	}

	public final void updateEventStatus(StoredTestEvent event, boolean success) throws IOException
	{
		logger.debug("Updating status of event {}", event.getId());
		doUpdateEventStatus(event, success);
		logger.debug("Status of event {} has been updated", event.getId());
	}

	public final CompletableFuture<Void> updateEventStatusAsync(StoredTestEvent event, boolean success)
	{
		logger.debug("Asynchronously updating status of event {}", event.getId());
		CompletableFuture<Void> result = doUpdateEventStatusAsync(event, success);
		result.whenCompleteAsync((r, error) -> {
				if (error != null)
					logger.error("Error while asynchronously updating status of event "+event.getId());
				else
					logger.debug("Status of event {} updated asynchronously", event.getId());
			}, composingService);
		return result;
	}
	
	
	private List<PageInfo> checkPages(List<PageToAdd> pages, BookInfo book) throws CradleStorageException
	{
		PageInfo lastPage = book.getLastPage();
		if (lastPage != null)  //If book has any pages, i.e. may have some data
		{
			Instant now = Instant.now(),
					firstStart = pages.get(0).getStart();
			if (!firstStart.isAfter(now))
				throw new CradleStorageException("Timestamp of new page start must be after current timestamp ("+now+")");
			if (!firstStart.isAfter(lastPage.getStarted()))
				throw new CradleStorageException("Timestamp of new page start must be after last page start ("+lastPage.getStarted()+")");
		}
		
		Set<String> names = new HashSet<>();
		PageToAdd prevPage = null;
		BookId bookId = book.getId();
		List<PageInfo> result = new ArrayList<>(pages.size());
		for (PageToAdd page : pages)
		{
			BookPagesNamesChecker.validatePageName(page.getName());

			String name = page.getName();
			if (names.contains(name))
				throw new CradleStorageException("Duplicated page name: '"+page.getName()+"'");
			names.add(name);
			
			if (book.getPage(new PageId(bookId, name)) != null)
				throw new CradleStorageException("Page '"+name+"' is already present in book '"+bookId+"'");
			
			if (prevPage != null)
			{
				if (!page.getStart().isAfter(prevPage.getStart()))
					throw new CradleStorageException("Unordered pages: page '"+name+"' should start after page '"+prevPage.getName()+"'");
				result.add(new PageInfo(new PageId(bookId, prevPage.getName()), 
						prevPage.getStart(), 
						page.getStart(), 
						prevPage.getComment()));
			}
			prevPage = page;
		}

		if (prevPage != null)
			result.add(new PageInfo(new PageId(bookId, prevPage.getName()), 
					prevPage.getStart(), 
					null, 
					prevPage.getComment()));
		return result;
	}
	
	private boolean checkFilter(MessageFilter filter) throws CradleStorageException
	{
		checkAbstractFilter(filter);
		
		//TODO: add more checks
		return true;
	}

	private void checkAbstractFilter(AbstractFilter filter) throws CradleStorageException
	{
		BookInfo book = bpc.getBook(filter.getBookId());
		if (filter.getPageId() != null)
			bpc.checkPage(filter.getPageId(), book.getId());
	}
	
	private boolean checkFilter(TestEventFilter filter) throws CradleStorageException
	{
		BookInfo book = bpc.getBook(filter.getBookId());
		checkAbstractFilter(filter);
		
		if (filter.getParentId() != null && !book.getId().equals(filter.getParentId().getBookId()))
			throw new CradleStorageException("Requested book ("+book.getId()+") doesn't match book of requested parent ("+filter.getParentId()+")");
		
		Instant timeFrom = filter.getStartTimestampFrom() != null ? filter.getStartTimestampFrom().getValue() : null,
				timeTo = filter.getStartTimestampTo() != null ? filter.getStartTimestampTo().getValue() : null;
		if (timeFrom != null && timeTo != null 
				&& timeFrom.isAfter(timeTo))
			throw new CradleStorageException("Left bound for start timestamp ("+timeFrom+") "
					+ "is after the right bound ("+timeTo+")");
		
		if (timeTo != null && timeTo.isBefore(book.getCreated()))
			return false;
		
		if (filter.getPageId() != null)
		{
			PageInfo page = book.getPage(filter.getPageId());
			Instant pageStarted = page.getStarted(),
					pageEnded = page.getEnded();
			
			if (timeFrom != null && pageEnded != null && timeFrom.isAfter(pageEnded))
				return false;
			if (timeTo != null && timeTo.isBefore(pageStarted))
				return false;
		}
		
		return true;
	}


}