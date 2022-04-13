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

package com.exactpro.cradle;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Dummy implementation of CradleStorage that does nothing and serves as a stub
 */
public class DummyCradleStorage extends CradleStorage
{
	class DummyBookCache implements BookCache {

		Map<BookId, BookInfo> books;

		DummyBookCache () {
			books = new ConcurrentHashMap<>();
		}

		@Override
		public BookInfo getBook(BookId bookId) throws CradleStorageException {
			if (!books.containsKey(bookId)) {
				throw new CradleStorageException(String.format("Book %s is unknown", bookId.getName()));
			}
			return books.get(bookId);
		}

		@Override
		public boolean checkBook(BookId bookId) {
			return false;
		}

		@Override
		public Collection<PageInfo> loadPageInfo(BookId bookId, boolean loadRemoved) throws CradleStorageException {
			return null;
		}

		@Override
		public BookInfo loadBook(BookId bookId) throws CradleStorageException {
			return null;
		}

		@Override
		public void updateCachedBook(BookInfo bookInfo) {
			books.put(bookInfo.getId(), bookInfo);
		}

		@Override
		public Collection<BookInfo> getCachedBooks() {
			return null;
		}
	}

	private final DummyBookCache dummyBookCache;

	@Override
	protected BookCache getBookCache() {
		return dummyBookCache;
	}

	public DummyCradleStorage() throws CradleStorageException
	{
		super();
		dummyBookCache = new DummyBookCache();
	}
	
	
	@Override
	protected void doInit(boolean prepareStorage) throws CradleStorageException
	{
	}
	
	@Override
	protected void doDispose() throws CradleStorageException
	{
	}

	@Override
	protected Collection<PageInfo> doGetAllPages(BookId bookId) {
		return null;
	}

	@Override
	protected Collection<BookListEntry> doListBooks() {
		return null;
	}

	@Override
	protected void doAddBook(BookToAdd newBook, BookId bookId)
	{
	}
	
	@Override
	protected void doAddPages(BookId bookId, List<PageInfo> pages, PageInfo lastPage)
			throws CradleStorageException, IOException
	{
	}
	
	@Override
	protected Collection<PageInfo> doLoadPages(BookId bookId) throws CradleStorageException, IOException
	{
		return null;
	}
	
	@Override
	protected void doRemovePage(PageInfo page) throws CradleStorageException, IOException
	{
	}
	
	@Override
	protected void doStoreMessageBatch(MessageBatchToStore batch, PageInfo page) throws IOException
	{
	}

	@Override
	protected void doStoreGroupedMessageBatch(MessageBatchToStore batch, PageInfo page, String groupName)
			throws IOException
	{
		
	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(MessageBatchToStore batch,
			PageInfo page)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CompletableFuture<Void> doStoreGroupedMessageBatchAsync(MessageBatchToStore batch, PageInfo page,
			String groupName) throws IOException, CradleStorageException
	{
		return null;
	}

	@Override
	protected void doStoreTestEvent(TestEventToStore event, PageInfo page) throws IOException
	{
	}
	
	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(TestEventToStore event, PageInfo page)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected void doUpdateParentTestEvents(TestEventToStore event) throws IOException
	{
	}
	
	@Override
	protected CompletableFuture<Void> doUpdateParentTestEventsAsync(TestEventToStore event)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected void doUpdateEventStatus(StoredTestEvent event, boolean success) throws IOException
	{
	}
	
	@Override
	protected CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEvent event, boolean success)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	
	@Override
	protected StoredMessage doGetMessage(StoredMessageId id, PageId pageId) throws IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id, PageId pageId)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected StoredMessageBatch doGetMessageBatch(StoredMessageId id, PageId pageId) throws IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<StoredMessageBatch> doGetMessageBatchAsync(StoredMessageId id, PageId pageId)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected CradleResultSet<StoredMessage> doGetMessages(MessageFilter filter, BookInfo book) throws IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<CradleResultSet<StoredMessage>> doGetMessagesAsync(MessageFilter filter,
			BookInfo book)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected CradleResultSet<StoredMessageBatch> doGetMessageBatches(MessageFilter filter, BookInfo book) throws IOException
	{
		return null;
	}

	@Override
	protected CradleResultSet<StoredMessageBatch> doGetGroupedMessageBatches(GroupedMessageFilter filter,
			BookInfo book)
			throws IOException, CradleStorageException
	{
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetMessageBatchesAsync(MessageFilter filter,
			BookInfo book)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetGroupedMessageBatchesAsync(
			GroupedMessageFilter filter, BookInfo book) throws CradleStorageException
	{
		return null;
	}


	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, BookId bookId) throws IOException
	{
		return 0;
	}

	@Override
	protected long doGetFirstSequence(String sessionAlias, Direction direction, BookId bookId)
			throws IOException, CradleStorageException
	{
		return 0;
	}

	@Override
	protected Collection<String> doGetSessionAliases(BookId bookId) throws IOException
	{
		return null;
	}
	
	
	@Override
	protected StoredTestEvent doGetTestEvent(StoredTestEventId id, PageId pageId) throws IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<StoredTestEvent> doGetTestEventAsync(StoredTestEventId ids, PageId pageId)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected CradleResultSet<StoredTestEvent> doGetTestEvents(TestEventFilter filter, BookInfo book) 
			throws CradleStorageException, IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<CradleResultSet<StoredTestEvent>> doGetTestEventsAsync(TestEventFilter filter, BookInfo book) 
			throws CradleStorageException
	{
		return CompletableFuture.completedFuture(null);
	}
	
	
	@Override
	protected Collection<String> doGetScopes(BookId bookId) throws IOException, CradleStorageException
	{
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<CounterSample>> doGetMessageCountersAsync(BookId bookId, String sessionAlias, Direction direction, FrameType frameType, Interval interval) throws CradleStorageException {
		return null;
	}

	@Override
	protected CradleResultSet<CounterSample> doGetMessageCounters(BookId bookId, String sessionAlias, Direction direction, FrameType frameType, Interval interval) throws CradleStorageException, IOException {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<CounterSample>> doGetCountersAsync(BookId bookId, EntityType entityType, FrameType frameType, Interval interval) throws CradleStorageException {
		return null;
	}

	@Override
	protected CradleResultSet<CounterSample> doGetCounters(BookId bookId, EntityType entityType, FrameType frameType, Interval interval) throws CradleStorageException, IOException {
		return null;
	}

	@Override
	protected CompletableFuture<Counter> doGetMessageCountAsync(BookId bookId, String sessionAlias, Direction direction, Interval interval) throws CradleStorageException {
		return null;
	}

	@Override
	protected Counter doGetMessageCount(BookId bookId, String sessionAlias, Direction direction, Interval interval) throws CradleStorageException, IOException {
		return null;
	}

	@Override
	protected CompletableFuture<Counter> doGetCountAsync(BookId bookId, EntityType entityType, Interval interval) throws CradleStorageException {
		return null;
	}

	@Override
	protected Counter doGetCount(BookId bookId, EntityType entityType, Interval interval) throws CradleStorageException, IOException {
		return null;
	}

	@Override
	public IntervalsWorker getIntervalsWorker(PageId pageId)
	{
		return null;
	}
}