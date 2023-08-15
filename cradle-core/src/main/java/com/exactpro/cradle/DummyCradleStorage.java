/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.counters.Counter;
import com.exactpro.cradle.counters.CounterSample;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventFilter;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dummy implementation of CradleStorage that does nothing and serves as a stub
 */
public class DummyCradleStorage extends CradleStorage {
	static class DummyBookCache implements BookCache {

		Map<BookId, BookInfo> books;

		DummyBookCache() {
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
		public Collection<PageInfo> loadPageInfo(BookId bookId, boolean loadRemoved) {
			return null;
		}

        @Override
		public BookInfo loadBook(BookId bookId) {
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
	protected void doInit(boolean prepareStorage) {
	}

	@Override
	protected void doDispose() {
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
	protected void doAddPages(BookId bookId, List<PageInfo> pages, PageInfo lastPage) {
	}

	@Override
	protected Collection<PageInfo> doLoadPages(BookId bookId) {
		return null;
	}

	@Override
	protected void doRemovePage(PageInfo page) {
	}

	@Override
	protected void doStoreMessageBatch(MessageBatchToStore batch, PageInfo page) {
	}

	@Override
	protected void doStoreGroupedMessageBatch(GroupedMessageBatchToStore batch, PageInfo page) {

	}

	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(MessageBatchToStore batch,
			PageInfo page)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CompletableFuture<Void> doStoreGroupedMessageBatchAsync(GroupedMessageBatchToStore batch, PageInfo page) {
		return null;
	}

	@Override
	protected void doStoreTestEvent(TestEventToStore event, PageInfo page) {
	}
	
	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(TestEventToStore event, PageInfo page)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected void doUpdateParentTestEvents(TestEventToStore event) {
	}
	
	@Override
	protected CompletableFuture<Void> doUpdateParentTestEventsAsync(TestEventToStore event)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected void doUpdateEventStatus(StoredTestEvent event, boolean success) {
	}
	
	@Override
	protected CompletableFuture<Void> doUpdateEventStatusAsync(StoredTestEvent event, boolean success)
	{
		return CompletableFuture.completedFuture(null);
	}


	@Override
	protected StoredMessage doGetMessage(StoredMessageId id, PageId pageId) {
		return null;
	}
	
	@Override
	protected CompletableFuture<StoredMessage> doGetMessageAsync(StoredMessageId id, PageId pageId)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected StoredMessageBatch doGetMessageBatch(StoredMessageId id, PageId pageId) {
		return null;
	}
	
	@Override
	protected CompletableFuture<StoredMessageBatch> doGetMessageBatchAsync(StoredMessageId id, PageId pageId)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CradleResultSet<StoredMessage> doGetMessages(MessageFilter filter, BookInfo book) {
		return null;
	}
	
	@Override
	protected CompletableFuture<CradleResultSet<StoredMessage>> doGetMessagesAsync(MessageFilter filter,
			BookInfo book)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CradleResultSet<StoredMessageBatch> doGetMessageBatches(MessageFilter filter, BookInfo book) {
		return null;
	}

	@Override
	protected CradleResultSet<StoredGroupedMessageBatch> doGetGroupedMessageBatches(GroupedMessageFilter filter,
																					BookInfo book) {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<StoredMessageBatch>> doGetMessageBatchesAsync(MessageFilter filter,
			BookInfo book)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CompletableFuture<CradleResultSet<StoredGroupedMessageBatch>> doGetGroupedMessageBatchesAsync(
			GroupedMessageFilter filter, BookInfo book) {
		return null;
	}


	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, BookId bookId) {
		return 0;
	}

	@Override
	protected long doGetFirstSequence(String sessionAlias, Direction direction, BookId bookId) {
		return 0;
	}

	@Override
	protected Collection<String> doGetSessionAliases(BookId bookId) {
		return null;
	}

	@Override
	protected Collection<String> doGetGroups(BookId bookId) {
		return null;
	}


	@Override
	protected StoredTestEvent doGetTestEvent(StoredTestEventId id, PageId pageId) {
		return null;
	}
	
	@Override
	protected CompletableFuture<StoredTestEvent> doGetTestEventAsync(StoredTestEventId ids, PageId pageId)
	{
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CradleResultSet<StoredTestEvent> doGetTestEvents(TestEventFilter filter, BookInfo book) {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<StoredTestEvent>> doGetTestEventsAsync(TestEventFilter filter, BookInfo book) {
		return CompletableFuture.completedFuture(null);
	}


	@Override
	protected Collection<String> doGetScopes(BookId bookId) {
		return null;
	}

	@Override
	protected CradleResultSet<String> doGetScopes(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<String>> doGetScopesAsync(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<CounterSample>> doGetMessageCountersAsync(BookId bookId, String sessionAlias, Direction direction, FrameType frameType, Interval interval) {
		return null;
	}

	@Override
	protected CradleResultSet<CounterSample> doGetMessageCounters(BookId bookId, String sessionAlias, Direction direction, FrameType frameType, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<CounterSample>> doGetCountersAsync(BookId bookId, EntityType entityType, FrameType frameType, Interval interval) {
		return null;
	}

	@Override
	protected CradleResultSet<CounterSample> doGetCounters(BookId bookId, EntityType entityType, FrameType frameType, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<Counter> doGetMessageCountAsync(BookId bookId, String sessionAlias, Direction direction, Interval interval) {
		return null;
	}

	@Override
	protected Counter doGetMessageCount(BookId bookId, String sessionAlias, Direction direction, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<Counter> doGetCountAsync(BookId bookId, EntityType entityType, Interval interval) {
		return null;
	}

	@Override
	protected Counter doGetCount(BookId bookId, EntityType entityType, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<String>> doGetSessionAliasesAsync(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	protected CradleResultSet<String> doGetSessionAliases(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<CradleResultSet<String>> doGetSessionGroupsAsync(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	protected CradleResultSet<String> doGetSessionGroups(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	protected PageInfo doUpdatePageComment(BookId bookId, String pageName, String comment) {
		return null;
	}

	@Override
	protected PageInfo doUpdatePageName(BookId bookId, String pageName, String newPageName) {
		return null;
	}

	@Override
	protected Iterator<PageInfo> doGetPages(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	protected CompletableFuture<Iterator<PageInfo>> doGetPagesAsync(BookId bookId, Interval interval) {
		return null;
	}

	@Override
	public IntervalsWorker getIntervalsWorker()
	{
		return null;
	}
}