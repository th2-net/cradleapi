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

package com.exactpro.cradle.messages;

/**
 * Metadata of message to store in Cradle
 */
public class MessageMetadata extends StoredMessageMetadata
{
	private static final long serialVersionUID = -5653282321813972483L;
	
	public MessageMetadata()
	{
	}
	
	public MessageMetadata(MessageMetadata metadata)
	{
		super(metadata);
	}
	
	
	public void add(String key, String value)
	{
		data.put(key, value);
	}
	
	public void remove(String key)
	{
		data.remove(key);
	}
}
