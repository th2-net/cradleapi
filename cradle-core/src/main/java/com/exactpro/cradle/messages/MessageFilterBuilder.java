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

import com.exactpro.cradle.*;
import com.exactpro.cradle.filters.*;
import com.exactpro.cradle.utils.CradleStorageException;

/**
 * Builder of filter for stored messages.
 * Various combinations of filter conditions may have different performance because some operations are done on client side.
 */
public class MessageFilterBuilder extends AbstractFilterBuilder<MessageFilterBuilder, MessageFilter>
{
	private String sessionAlias;
	private Direction direction;
	private FilterForAny<Long> sequence;

	public MessageFilterBuilder()
	{
	}
	
	public static MessageFilterBuilder next(StoredMessageId messageId, int limit)
	{
		return new MessageFilterBuilder()
				.bookId(messageId.getBookId())
				.sessionAlias(messageId.getSessionAlias())
				.direction(messageId.getDirection())
				.timestampFrom().isGreaterThan(messageId.getTimestamp())
				.sequence().isGreaterThan(messageId.getSequence())
				.limit(limit);
	}
	
	public static MessageFilterBuilder previous(StoredMessageId messageId, int limit)
	{
		return new MessageFilterBuilder()
				.bookId(messageId.getBookId())
				.sessionAlias(messageId.getSessionAlias())
				.direction(messageId.getDirection())
				.timestampTo().isLessThan(messageId.getTimestamp())
				.sequence().isLessThan(messageId.getSequence())
				.limit(limit);
	}
	
	
	public MessageFilterBuilder sessionAlias(String sessionAlias)
	{
		this.sessionAlias = sessionAlias;
		return this;
	}
	
	public MessageFilterBuilder direction(Direction direction)
	{
		this.direction = direction;
		return this;
	}
	
	public FilterForAnyBuilder<Long, MessageFilterBuilder> sequence()
	{
		FilterForAny<Long> f = new FilterForAny<>();
		sequence = f;
		return new FilterForAnyBuilder<>(f, this);
	}
	
	@Override
	public MessageFilter build() throws CradleStorageException
	{
		try
		{
			MessageFilter result = super.build();
			result.setSequence(sequence);
			return result;
		}
		finally
		{
			reset();
		}
	}

	@Override
	protected MessageFilter createFilterInstance() throws CradleStorageException
	{
		return new MessageFilter(getBookId(), sessionAlias, direction, getPageId());
	}

	protected void reset()
	{
		super.reset();
		sessionAlias = null;
		direction = null;
		sequence = null;
	}
}