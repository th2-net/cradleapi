/*
 *  Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.cradle.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.google.common.collect.Lists;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.exactpro.cradle.serialization.MessagesSizeCalculator.MESSAGE_BATCH_CONST_VALUE;

public class StoredGroupedMessageBatch {
	protected BookId bookId;
	private final String group;
	protected int batchSize;
    protected final List<StoredMessage> messages;
	private final Instant recDate;

	public BookId getBookId() {
		return bookId;
	}

	public StoredGroupedMessageBatch(String group) {
		this(group, null, null, null);
	}

	public StoredGroupedMessageBatch(String group, Collection<StoredMessage> messages, PageId pageId, Instant recDate) {
		this.recDate = recDate;
		this.group = group;
		this.messages = createMessagesCollection(messages, pageId);
		if (messages == null || messages.isEmpty()) {
            batchSize = MESSAGE_BATCH_CONST_VALUE;
			return;
		}
		batchSize = MessagesSizeCalculator.calculateMessageBatchSize(messages).total;
	}

	public String getGroup() {
		return group;
	}
	public int getMessageCount() {
		return messages.size();
	}

	public int getBatchSize() {
		return batchSize;
	}

	public Collection<StoredMessage> getMessages() {
		return Collections.unmodifiableCollection(messages);
	}

	public Collection<StoredMessage> getMessagesReverse()	{
        return Collections.unmodifiableCollection(Lists.reverse(messages));
	}

	public StoredMessage getFirstMessage() {
        return !messages.isEmpty() ? messages.get(0) : null;
	}

	public StoredMessage getLastMessage() {
        return !messages.isEmpty() ? messages.get(messages.size() - 1) : null;
	}

	public Instant getFirstTimestamp() {
		StoredMessage m = getFirstMessage();
		return m != null ? m.getTimestamp() : null;
	}

	public Instant getLastTimestamp() {
		StoredMessage m = getLastMessage();
		return m != null ? m.getTimestamp() : null;
	}


	public Instant getRecDate() {
		return recDate;
	}

	public boolean isEmpty() {
		return messages.isEmpty();
	}

    protected List<StoredMessage> createMessagesCollection(Collection<StoredMessage> messages, PageId pageId) {
		if (messages == null)
            return new ArrayList<>();
		
        List<StoredMessage> result = new ArrayList<>(messages.size());
		for (StoredMessage msg : messages)
            result.add(new StoredMessage(msg, msg.getId(), pageId)); // why do we copy messages here?
		return result;
	}
}
