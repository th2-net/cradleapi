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
import java.util.Objects;
import java.util.stream.Stream;

import static com.exactpro.cradle.serialization.MessagesSizeCalculator.MESSAGE_BATCH_CONST_VALUE;
import static java.util.stream.Collectors.toCollection;

public class StoredGroupedMessageBatch {
    protected BookId bookId;
    private final String group;
    protected int batchSize;
    private final List<StoredMessage> messages;
    private StoredMessage firstMessage;
    private StoredMessage lastMessage;
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
        this.messages = messages == null || messages.isEmpty()
                ? new ArrayList<>()
                : messages.stream()
                .map(msg -> Objects.equals(msg.getPageId(), pageId)
                        ? msg
                        : new StoredMessage(msg, msg.getId(), pageId)
                ).collect(toCollection(ArrayList<StoredMessage>::new));
        if (this.messages.isEmpty()) {
            batchSize = MESSAGE_BATCH_CONST_VALUE;
            return;
        }
        this.messages.forEach(this::updateFirstLast);
        batchSize = MessagesSizeCalculator.calculateMessageBatchSize(this.messages);
    this.bookId = pageId.getBookId();
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

    public Collection<StoredMessage> getMessagesReverse() {
        return Collections.unmodifiableCollection(Lists.reverse(messages));
    }

    public StoredMessage getFirstMessage() {
        return firstMessage;
    }

    public StoredMessage getLastMessage() {
        return lastMessage;
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

    protected Stream<StoredMessage> messagesStream() {
        return messages.stream();
    }

    protected void addMessage(StoredMessage message) {
        messages.add(message);
        updateFirstLast(message);
    }

    protected void addMessages(Collection<StoredMessage> messages) {
        this.messages.addAll(messages);
        messages.forEach(this::updateFirstLast);
    }

    private void updateFirstLast(StoredMessage message) {
        if (firstMessage == null || message.getTimestamp().isBefore(firstMessage.getTimestamp())) {
            firstMessage = message;
        }
        if (lastMessage == null || message.getTimestamp().isAfter(lastMessage.getTimestamp())) {
            lastMessage = message;
        }
    }

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof StoredGroupedMessageBatch)) return false;
		StoredGroupedMessageBatch that = (StoredGroupedMessageBatch) o;
		return getBatchSize() == that.getBatchSize()
				&& getBookId().equals(that.getBookId())
				&& getGroup().equals(that.getGroup())
				&& getMessages().equals(that.getMessages())
				&& getRecDate().equals(that.getRecDate());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBookId(), getGroup(), getBatchSize(), getMessages(), getRecDate());
	}

	@Override
	public String toString() {
		return "StoredGroupedMessageBatch{" +
				"bookId=" + bookId +
				", group='" + group + '\'' +
				", batchSize=" + batchSize +
				", messages=" + messages +
				", recDate=" + recDate +
				'}';
	}
}