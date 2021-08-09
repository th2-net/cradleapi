/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import java.util.concurrent.ConcurrentHashMap;

import com.exactpro.cradle.intervals.IntervalsWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventFilter;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.NoStorageException;
import com.exactpro.cradle.utils.TestEventUtils;

/**
 * Storage which holds information about all data sent or received and test events.
 */
public abstract class CradleStorage
{
	private static final Logger logger = LoggerFactory.getLogger(CradleStorage.class);
	public static final ZoneOffset TIMEZONE_OFFSET = ZoneOffset.UTC;
	
	private final Map<BookId, BookInfo> books;
	private final BookAndPageChecker bpc;
	private volatile boolean disposed = false;
	
	public CradleStorage() throws CradleStorageException
	{
		books = new ConcurrentHashMap<>();
		try
		{
			Collection<BookInfo> loaded = loadBooks();
			if (loaded != null)
				loaded.forEach((b) -> books.put(b.getId(), b));
		}
		catch (NoStorageException e)
		{
			logger.warn("Could not load books. Internal storage is not initialized. Call createStorage and createBook methods", e);
		}
		
		bpc = new BookAndPageChecker(books);
	}
	
	
	protected abstract Collection<BookInfo> loadBooks() throws NoStorageException, CradleStorageException;
	protected abstract void writeBook(BookInfo newBook) throws NoStorageException, CradleStorageException;
	protected abstract void doSwitchToNextPage(BookId bookId, String pageName, Instant timestamp) throws CradleStorageException;
	protected abstract void doDispose() throws CradleStorageException;
	
	
	protected abstract void doStoreMessageBatch(StoredMessageBatch batch) throws IOException;
	protected abstract CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch);
	
	
	protected abstract void doStoreTestEvent(StoredTestEvent event) throws IOException;
	protected abstract CompletableFuture<Void> doStoreTestEventAsync(StoredTestEvent event);
	protected abstract void doUpdateParentTestEvents(StoredTestEvent event) throws IOException;
	protected abstract CompletableFuture<Void> doUpdateParentTestEventsAsync(StoredTestEvent event);
	protected abstract void doUpdateEventStatus(StoredTestEvent event, boolean success) throws IOException;
	protected abstract CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEvent event, boolean success);
	
	
	protected abstract StoredMessage doGetMessage(StoredMessageId id, PageId pageId) throws IOException;
	protected abstract CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id, PageId pageId);
	protected abstract Collection<StoredMessage> doGetMessageBatch(StoredMessageId id, PageId pageId) throws IOException;
	protected abstract CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id, PageId pageId);
	
	protected abstract Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException;
	protected abstract CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter);
	protected abstract Iterable<StoredMessageBatch> doGetMessagesBatches(StoredMessageFilter filter) throws IOException;
	protected abstract CompletableFuture<Iterable<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter);
	
	protected abstract long doGetLastSequence(String sessionAlias, Direction direction, PageId pageId) throws IOException;
	protected abstract Collection<String> doGetSessionAliases(PageId pageId) throws IOException;
	
	
	protected abstract StoredTestEvent doGetTestEvent(StoredTestEventId id, PageId pageId) throws IOException;
	protected abstract CompletableFuture<StoredTestEvent> doGetTestEventAsync(StoredTestEventId ids, PageId pageId);
	
	protected abstract Iterable<StoredTestEvent> doGetTestEvents(StoredTestEventFilter filter) throws CradleStorageException, IOException;
	protected abstract CompletableFuture<Iterable<StoredTestEvent>> doGetTestEventsAsync(StoredTestEventFilter filter) throws CradleStorageException, IOException;
	
	/**
	 * Initializes internal storage, i.e. creates database, file, etc. depending on implementation
	 * @throws CradleStorageException
	 */
	public abstract void createStorage() throws CradleStorageException;
	
	/**
	 * IntervalsWorker is used to work with Crawler intervals
	 * @return instance of IntervalsWorker
	 */
	public abstract IntervalsWorker getIntervalsWorker();
	
	
	/**
	 * Disposes resources occupied by storage which means closing of opened connections, flushing all buffers, etc.
	 * @throws CradleStorageException if there was error during storage disposal, which may mean issue with data flushing, unexpected connection break, etc.
	 */
	public final void dispose() throws CradleStorageException
	{
		logger.info("Disposing storage");
		disposed = true;
		doDispose();
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
	 * Creates book in storage, adding page with given name to newly created book
	 * @param name short name of book to create
	 * @param fullName long name
	 * @param desc description of the book
	 * @param firstPageName name of first page to add to new book
	 * @return {@link BookInfo} containing all information about created book
	 * @throws NoStorageException if no storage is available
	 * @throws CradleStorageException if error occurred while creating new book
	 */
	public BookInfo createBook(String name, String fullName, String desc, String firstPageName) throws NoStorageException, CradleStorageException
	{
		BookId id = new BookId(name);
		logger.info("Adding book '{}' to storage", id);
		if (books.containsKey(id))
			throw new CradleStorageException("Book '"+id+"' is already present in storage");
		
		BookInfo newBook = new BookInfo(id, fullName, desc, Instant.now());
		writeBook(newBook);
		books.put(newBook.getId(), newBook);
		logger.info("Book '{}' has been added to storage", id);
		switchToNextPage(id, firstPageName);
		return newBook;
	}
	
	/**
	 * @return collection of books currently available in storage
	 */
	public Collection<BookInfo> getBooks()
	{
		return Collections.unmodifiableCollection(books.values());
	}
	
	public void switchToNextPage(BookId bookId, String pageName) throws CradleStorageException
	{
		//TODO: check and fix this method
		logger.info("Switching to page '{}' of book '{}'", pageName, bookId);
		BookInfo book = bpc.getBook(bookId);
		Instant now = Instant.now();
		doSwitchToNextPage(bookId, pageName, now);
		book.nextPage(pageName, now);
	}
	
	
	/**
	 * Writes data about given message batch to current page
	 * @param batch data to write
	 * @throws IOException if data writing failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final void storeMessageBatch(StoredMessageBatch batch) throws IOException, CradleStorageException
	{
		StoredMessageId id = batch.getId();
		logger.debug("Storing message batch {}", id);
		bpc.checkActivePage(id.getBookId(), id.getTimestamp());
		doStoreMessageBatch(batch);
		logger.debug("Message batch {} has been stored", id);
	}
	
	
	/**
	 * Asynchronously writes data about given message batch to current page
	 * @param batch data to write
	 * @return future to get know if storing was successful
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Void> storeMessageBatchAsync(StoredMessageBatch batch) throws CradleStorageException
	{
		StoredMessageId id = batch.getId();
		logger.debug("Storing message batch {} asynchronously", id);
		bpc.checkActivePage(id.getBookId(), id.getTimestamp());
		CompletableFuture<Void> result = doStoreMessageBatchAsync(batch);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while storing message batch "+id+" asynchronously", error);
				else
					logger.debug("Message batch {} has been stored asynchronously", id);
			});
		return result;
	}
	
	
	/**
	 * Writes data about given test event to current page
	 * @param event data to write
	 * @throws IOException if data writing failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final void storeTestEvent(StoredTestEvent event) throws IOException, CradleStorageException
	{
		StoredTestEventId id = event.getId();
		logger.debug("Storing test event {}", id);
		bpc.checkActivePage(id.getBookId(), id.getStartTimestamp());
		
		TestEventUtils.validateTestEvent(event, !(event instanceof StoredTestEventBatch));
		
		doStoreTestEvent(event);
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
	public final CompletableFuture<Void> storeTestEventAsync(StoredTestEvent event) throws IOException, CradleStorageException
	{
		StoredTestEventId id = event.getId();
		logger.debug("Storing test event {} asynchronously", id);
		bpc.checkActivePage(id.getBookId(), id.getStartTimestamp());
		
		TestEventUtils.validateTestEvent(event, !(event instanceof StoredTestEventBatch));
		
		CompletableFuture<Void> result = doStoreTestEventAsync(event);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while storing test event "+id+" asynchronously", error);
				else
					logger.debug("Test event {} has been stored asynchronously", id);
			});
		
		if (event.getParentId() == null)
			return result;
		
		return result.thenComposeAsync(r -> {
			logger.debug("Updating parents of test event {} asynchronously", id);
			CompletableFuture<Void> result2 = doUpdateParentTestEventsAsync(event);
			result2.whenComplete((r2, error) -> {
					if (error != null)
						logger.error("Error while updating parents of test event "+id+" asynchronously", error);
					else
						logger.debug("Parents of test event {} have been updated asynchronously", event.getId());
				});
			return result2;
		});
	}
	
	
	/**
	 * Retrieves message data stored under given ID in given page
	 * @param id of stored message to retrieve
	 * @param pageId to get message from
	 * @return data of stored message
	 * @throws IOException if message data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final StoredMessage getMessage(StoredMessageId id, PageId pageId) throws IOException, CradleStorageException
	{
		logger.debug("Getting message {} from page {}", id, pageId);
		bpc.checkPage(pageId, id.getBookId());
		StoredMessage result = doGetMessage(id, pageId);
		logger.debug("Message {} from page {} got", id, pageId);
		return result;
	}
	
	/**
	 * Retrieves message data stored under given ID in current page
	 * @param id of stored message to retrieve
	 * @return data of stored message
	 * @throws IOException if message data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final StoredMessage getMessage(StoredMessageId id) throws IOException, CradleStorageException
	{
		return getMessage(id, bpc.getActivePageId(id.getBookId()));
	}
	
	/**
	 * Asynchronously retrieves message data stored under given ID in given page
	 * @param id of stored message to retrieve
	 * @param pageId to get message from
	 * @return future to obtain data of stored message
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<StoredMessage> getMessageAsync(StoredMessageId id, PageId pageId) throws CradleStorageException
	{
		logger.debug("Getting message {} from page {} asynchronously", id, pageId);
		bpc.checkPage(pageId, id.getBookId());
		CompletableFuture<StoredMessage> result = doGetMessageAsync(id, pageId);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting message "+id+" from page "+pageId+" asynchronously", error);
				else
					logger.debug("Message {} from page {} got asynchronously", id, pageId);
			});
		return result;
	}
	
	/**
	 * Asynchronously retrieves message data stored under given ID in current page
	 * @param id of stored message to retrieve
	 * @return future to obtain data of stored message
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<StoredMessage> getMessageAsync(StoredMessageId id) throws CradleStorageException
	{
		return getMessageAsync(id, bpc.getActivePageId(id.getBookId()));
	}
	
	
	/**
	 * Retrieves from given page the batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @param pageId to get batch from
	 * @return collection with messages stored in batch
	 * @throws IOException if batch data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final Collection<StoredMessage> getMessageBatch(StoredMessageId id, PageId pageId) throws IOException, CradleStorageException
	{
		logger.debug("Getting message batch by message ID {} from page {}", id, pageId);
		bpc.checkPage(pageId, id.getBookId());
		Collection<StoredMessage> result = doGetMessageBatch(id, pageId);
		logger.debug("Message batch by message ID {} from page {} got", id, pageId);
		return result;
	}
	
	/**
	 * Retrieves from current page the batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @return collection with messages stored in batch
	 * @throws IOException if batch data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final Collection<StoredMessage> getMessageBatch(StoredMessageId id) throws IOException, CradleStorageException
	{
		return getMessageBatch(id, bpc.getActivePageId(id.getBookId()));
	}
	
	/**
	 * Asynchronously retrieves from given page the batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @param pageId to get batch from
	 * @return future to obtain collection with messages stored in batch
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Collection<StoredMessage>> getMessageBatchAsync(StoredMessageId id, PageId pageId) throws CradleStorageException
	{
		logger.debug("Getting message batch by message ID {} from page {} asynchronously", id, pageId);
		bpc.checkPage(pageId, id.getBookId());
		CompletableFuture<Collection<StoredMessage>> result = doGetMessageBatchAsync(id, pageId);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting message batch by message ID "+id+" from page "+pageId+" asynchronously", error);
				else
					logger.debug("Message batch by message ID {} from page {} got asynchronously", id, pageId);
			});
		return result;
	}
	
	/**
	 * Asynchronously retrieves from current page the batch of messages where message with given ID is stored
	 * @param id of stored message whose batch to retrieve
	 * @return future to obtain collection with messages stored in batch
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Collection<StoredMessage>> getMessageBatchAsync(StoredMessageId id) throws CradleStorageException
	{
		return getMessageBatchAsync(id, bpc.getActivePageId(id.getBookId()));
	}
	
	
	/**
	 * Allows to enumerate stored messages filtering them by given conditions
	 * @param filter defines conditions to filter messages by
	 * @return iterable object to enumerate messages
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final Iterable<StoredMessage> getMessages(StoredMessageFilter filter) throws IOException, CradleStorageException
	{
		logger.debug("Filtering messages by {}", filter);
		bpc.checkPage(filter.getPageId());
		Iterable<StoredMessage> result = doGetMessages(filter);
		logger.debug("Prepared iterator for messages filtered by {}", filter);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate stored messages filtering them by given conditions
	 * @param filter defines conditions to filter messages by
	 * @return future to obtain iterable object to enumerate messages
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredMessage>> getMessagesAsync(StoredMessageFilter filter) throws CradleStorageException
	{
		logger.debug("Asynchronously getting messages filtered by {}", filter);
		bpc.checkPage(filter.getPageId());
		CompletableFuture<Iterable<StoredMessage>> result = doGetMessagesAsync(filter);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting messages filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Iterator for messages filtered by {} got asynchronously", filter);
			});
		return result;
	}
	
	
	/**
	 * Allows to enumerate stored message batches filtering them by given conditions
	 * @param filter defines conditions to filter message batches by
	 * @return iterable object to enumerate message batches
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final Iterable<StoredMessageBatch> getMessagesBatches(StoredMessageFilter filter) throws IOException, CradleStorageException
	{
		logger.debug("Filtering message batches by {}", filter);
		bpc.checkPage(filter.getPageId());
		Iterable<StoredMessageBatch> result = doGetMessagesBatches(filter);
		logger.debug("Prepared iterator for message batches filtered by {}", filter);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate stored message batches filtering them by given conditions
	 * @param filter defines conditions to filter message batches by
	 * @return future to obtain iterable object to enumerate message batches
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredMessageBatch>> getMessagesBatchesAsync(StoredMessageFilter filter) throws CradleStorageException
	{
		logger.debug("Asynchronously getting message batches filtered by {}", filter);
		bpc.checkPage(filter.getPageId());
		CompletableFuture<Iterable<StoredMessageBatch>> result = doGetMessagesBatchesAsync(filter);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting message batches filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Iterator for message batches filtered by {} got asynchronously", filter);
			});
		return result;
	}
	
	
	/**
	 * Retrieves last stored sequence number for given session alias and direction within given page. 
	 * Use result of this method to continue writing messages.
	 * @param sessionAlias to get sequence number for 
	 * @param direction to get sequence number for
	 * @param pageId to search in
	 * @return last stored sequence number for given arguments, if it is present, -1 otherwise
	 * @throws IOException if retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final long getLastSequence(String sessionAlias, Direction direction, PageId pageId) throws IOException, CradleStorageException
	{
		logger.debug("Getting last stored sequence number for session alias '{}' and direction '{}', page {}", sessionAlias, direction.getLabel(), pageId);
		bpc.checkPage(pageId);
		long result = doGetLastSequence(sessionAlias, direction, pageId);
		logger.debug("Sequence number {} got", result);
		return result;
	}
	
	
	/**
	 * Obtains collection of session aliases whose messages are saved in given page
	 * @param pageId to get session aliases from
	 * @return collection of session aliases
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final Collection<String> getSessionAliases(PageId pageId) throws IOException, CradleStorageException
	{
		logger.debug("Getting session aliases");
		bpc.checkPage(pageId);
		Collection<String> result = doGetSessionAliases(pageId);
		logger.debug("Session aliases got");
		return result;
	}
	
	
	/**
	 * Retrieves test event data stored under given ID in given page
	 * @param id of stored test event to retrieve
	 * @param pageId to get test event from
	 * @return data of stored test event
	 * @throws IOException if test event data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final StoredTestEvent getTestEvent(StoredTestEventId id, PageId pageId) throws IOException, CradleStorageException
	{
		logger.debug("Getting test event {} from page {}", id, pageId);
		bpc.checkPage(pageId, id.getBookId());
		StoredTestEvent result = doGetTestEvent(id, pageId);
		logger.debug("Test event {} from page {} got", id, pageId);
		return result;
	}
	
	/**
	 * Retrieves test event data stored under given ID in current page
	 * @param id of stored test event to retrieve
	 * @return data of stored test event
	 * @throws IOException if test event data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final StoredTestEvent getTestEvent(StoredTestEventId id) throws IOException, CradleStorageException
	{
		return getTestEvent(id, bpc.getActivePageId(id.getBookId()));
	}
	
	/**
	 * Asynchronously retrieves test event data stored under given ID in given page
	 * @param id of stored test event to retrieve
	 * @param pageId to get test event from
	 * @return future to obtain data of stored test event
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<StoredTestEvent> getTestEventAsync(StoredTestEventId id, PageId pageId) throws CradleStorageException
	{
		logger.debug("Getting test event {} from page {} asynchronously", id, pageId);
		bpc.checkPage(pageId, id.getBookId());
		CompletableFuture<StoredTestEvent> result = doGetTestEventAsync(id, pageId);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting test event "+id+" from page "+pageId+" asynchronously", error);
				else
					logger.debug("Test event {} from page {} got asynchronously", id, pageId);
			});
		return result;
	}
	
	/**
	 * Asynchronously retrieves test event data stored under given ID in current page
	 * @param id of stored test event to retrieve
	 * @return future to obtain data of stored test event
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<StoredTestEvent> getTestEventAsync(StoredTestEventId id) throws CradleStorageException
	{
		return getTestEventAsync(id, bpc.getActivePageId(id.getBookId()));
	}
	
	
	/**
	 * Allows to enumerate test events filtering them by given conditions
	 * @param filter defines conditions to filter test events by
	 * @return iterable object to enumerate test events
	 * @throws CradleStorageException if provided argument is invalid
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final Iterable<StoredTestEvent> getTestEvents(StoredTestEventFilter filter) throws CradleStorageException, IOException
	{
		logger.debug("Filtering test events by {}", filter);
		bpc.checkPage(filter.getPageId());
		Iterable<StoredTestEvent> result = doGetTestEvents(filter);
		logger.debug("Prepared iterator for test events filtered by {}", filter);
		return result;
	}
	
	/**
	 * Allows to asynchronously obtain iterable object to enumerate test events filtering them by given conditions
	 * @param filter defines conditions to filter test events by
	 * @return future to obtain iterable object to enumerate test events
	 * @throws CradleStorageException if provided argument is invalid
	 * @throws IOException if data retrieval failed
	 * @throws CradleStorageException if given parameters are invalid
	 */
	public final CompletableFuture<Iterable<StoredTestEvent>> getTestEventsAsync(StoredTestEventFilter filter) throws CradleStorageException, IOException
	{
		logger.debug("Asynchronously getting test events filtered by {}", filter);
		bpc.checkPage(filter.getPageId());
		CompletableFuture<Iterable<StoredTestEvent>> result = doGetTestEventsAsync(filter);
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while getting test events filtered by "+filter+" asynchronously", error);
				else
					logger.debug("Iterator for test events filtered by {} got asynchronously", filter);
			});
		return result;
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
		result.whenComplete((r, error) -> {
				if (error != null)
					logger.error("Error while asynchronously updating status of event "+event.getId());
				else
					logger.debug("Status of event {} updated asynchronously", event.getId());
			});
		return result;
	}
}