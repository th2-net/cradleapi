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

package com.exactpro.cradle.cassandra.linkers;

import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventConverter;
import com.exactpro.cradle.cassandra.dao.messages.MessageTestEventOperator;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventMessagesOperator;
import com.exactpro.cradle.cassandra.dao.testevents.converters.TestEventMessagesConverter;

public class LinkerSupplies
{
	private final TestEventMessagesOperator testEventsOperator;
	private final MessageTestEventOperator messagesOperator;
	private final TestEventMessagesConverter testEventConverter;
	private final MessageTestEventConverter messageConverter;
	
	public LinkerSupplies(TestEventMessagesOperator testEventsOperator, MessageTestEventOperator messagesOperator,
			TestEventMessagesConverter testEventConverter, MessageTestEventConverter messageConverter)
	{
		this.testEventsOperator = testEventsOperator;
		this.messagesOperator = messagesOperator;
		this.testEventConverter = testEventConverter;
		this.messageConverter = messageConverter;
	}
	
	
	public TestEventMessagesOperator getTestEventsOperator()
	{
		return testEventsOperator;
	}
	
	public MessageTestEventOperator getMessagesOperator()
	{
		return messagesOperator;
	}
	
	public TestEventMessagesConverter getTestEventConverter()
	{
		return testEventConverter;
	}
	
	public MessageTestEventConverter getMessageConverter()
	{
		return messageConverter;
	}
}
