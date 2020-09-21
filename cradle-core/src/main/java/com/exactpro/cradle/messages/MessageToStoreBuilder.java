/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import java.time.Instant;

import com.exactpro.cradle.Direction;

/**
 * Builder for MessageToStore object. After calling {@link #build()} method, the builder can be reused to build new message
 */
public class MessageToStoreBuilder
{
	private MessageToStore msg;
	
	public MessageToStoreBuilder()
	{
		msg = createMessageToStore();
	}
	
	
	protected MessageToStore createMessageToStore()
	{
		return new MessageToStore();
	}
	
	private void initIfNeeded()
	{
		if (msg == null)
			msg = createMessageToStore();
	}
	
	
	public MessageToStoreBuilder streamName(String streamName)
	{
		initIfNeeded();
		msg.setStreamName(streamName);
		return this;
	}
	
	public MessageToStoreBuilder direction(Direction d)
	{
		initIfNeeded();
		msg.setDirection(d);
		return this;
	}
	
	public MessageToStoreBuilder index(long index)
	{
		initIfNeeded();
		msg.setIndex(index);
		return this;
	}
	
	public MessageToStoreBuilder timestamp(Instant timestamp)
	{
		initIfNeeded();
		msg.setTimestamp(timestamp);
		return this;
	}
	
	public MessageToStoreBuilder metadata(String key, String value)
	{
		initIfNeeded();
		msg.addMetadata(key, value);
		return this;
	}
	
	public MessageToStoreBuilder content(byte[] content)
	{
		initIfNeeded();
		msg.setContent(content);
		return this;
	}
	
	
	public MessageToStore build()
	{
		initIfNeeded();
		MessageToStore result = msg;
		msg = null;
		return result;
	}
}
