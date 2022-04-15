/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class StoredMessageBatch implements MessageBatch
{
	protected StoredMessageId id;
	protected int batchSize;
	protected final List<StoredMessage> messages;
	private final Instant recDate;

	public StoredMessageBatch()
	{
		this(null, null, null);
	}

	public StoredMessageBatch(Collection<StoredMessage> messages, PageId pageId, Instant recDate)
	{
		this.recDate = recDate;
		this.messages = createMessagesList(messages, pageId);
		if (messages == null || messages.isEmpty()) {
			batchSize = MessagesSizeCalculator.calculateMessageBatchSize(Collections.emptyList()).total;
			return;
		}
		id = this.messages.get(0).getId();
		batchSize = MessagesSizeCalculator.calculateMessageBatchSize(messages).total;
	}

	@Override
	public StoredMessageId getId()
	{
		return id;
	}

	@Override
	public String getSessionAlias()
	{
		return id != null ? id.getSessionAlias() : null;
	}

	@Override
	public Direction getDirection()
	{
		return id != null ? id.getDirection() : null;
	}

	@Override
	public int getMessageCount()
	{
		return messages.size();
	}

	@Override
	public int getBatchSize()
	{
		return batchSize;
	}

	@Override
	public Collection<StoredMessage> getMessages()
	{
		return Collections.unmodifiableList(messages);
	}

	@Override
	public Collection<StoredMessage> getMessagesReverse()
	{
		List<StoredMessage> list = new ArrayList<>(messages);
		Collections.reverse(list);

		return Collections.unmodifiableList(list);
	}

	@Override
	public StoredMessage getFirstMessage()
	{
		return !messages.isEmpty() ? messages.get(0) : null;
	}

	@Override
	public StoredMessage getLastMessage()
	{
		return !messages.isEmpty() ? messages.get(messages.size()-1) : null;
	}

	@Override
	public Instant getFirstTimestamp()
	{
		StoredMessage m = getFirstMessage();
		return m != null ? m.getTimestamp() : null;
	}

	@Override
	public Instant getLastTimestamp()
	{
		StoredMessage m = getLastMessage();
		return m != null ? m.getTimestamp() : null;
	}


	public Instant getRecDate() {
		return recDate;
	}

	@Override
	public boolean isEmpty()
	{
		return messages.isEmpty();
	}

	protected List<StoredMessage> createMessagesList(Collection<StoredMessage> messages, PageId pageId)
	{
		if (messages == null)
			return new ArrayList<>();
		
		List<StoredMessage> result = new ArrayList<>(messages.size());
		for (StoredMessage msg : messages)
			result.add(new StoredMessage(msg, msg.getId(), pageId));
		return result;
	}
}
