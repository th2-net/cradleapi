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
import java.util.concurrent.CompletableFuture;

import com.exactpro.cradle.intervals.IntervalsWorker;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventFilter;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Dummy implementation of CradleStorage that does nothing and serves as a stub
 */
public class DummyCradleStorage extends CradleStorage
{
	public DummyCradleStorage() throws CradleStorageException
	{
		super();
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
	protected Collection<BookInfo> loadBooks() throws CradleStorageException
	{
		return null;
	}

	@Override
	protected void doAddBook(BookInfo newBook) throws CradleStorageException
	{
	}
	
	@Override
	protected void doSwitchToNextPage(BookId bookId, String pageName, Instant timestamp) throws CradleStorageException
	{
	}
	
	@Override
	protected void doStoreMessageBatch(StoredMessageBatch batch) throws IOException
	{
	}
	
	@Override
	protected CompletableFuture<Void> doStoreMessageBatchAsync(StoredMessageBatch batch)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected void doStoreTestEvent(StoredTestEvent event) throws IOException
	{
	}
	
	@Override
	protected CompletableFuture<Void> doStoreTestEventAsync(StoredTestEvent event)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected void doUpdateParentTestEvents(StoredTestEvent event) throws IOException
	{
	}
	
	@Override
	protected CompletableFuture<Void> doUpdateParentTestEventsAsync(StoredTestEvent event)
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
	protected Collection<StoredMessage> doGetMessageBatch(StoredMessageId id, PageId pageId) throws IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<Collection<StoredMessage>> doGetMessageBatchAsync(StoredMessageId id, PageId pageId)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected Iterable<StoredMessage> doGetMessages(StoredMessageFilter filter) throws IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<Iterable<StoredMessage>> doGetMessagesAsync(StoredMessageFilter filter)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	protected Iterable<StoredMessageBatch> doGetMessagesBatches(StoredMessageFilter filter) throws IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<Iterable<StoredMessageBatch>> doGetMessagesBatchesAsync(StoredMessageFilter filter)
	{
		return CompletableFuture.completedFuture(null);
	}
	
	
	@Override
	protected long doGetLastSequence(String sessionAlias, Direction direction, PageId pageId) throws IOException
	{
		return 0;
	}
	
	@Override
	protected Collection<String> doGetSessionAliases(PageId pageId) throws IOException
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
	protected Iterable<StoredTestEvent> doGetTestEvents(StoredTestEventFilter filter) throws CradleStorageException, IOException
	{
		return null;
	}
	
	@Override
	protected CompletableFuture<Iterable<StoredTestEvent>> doGetTestEventsAsync(StoredTestEventFilter filter) throws CradleStorageException, IOException
	{
		return CompletableFuture.completedFuture(null);
	}
	
	@Override
	public IntervalsWorker getIntervalsWorker(PageId pageId)
	{
		return null;
	}
}