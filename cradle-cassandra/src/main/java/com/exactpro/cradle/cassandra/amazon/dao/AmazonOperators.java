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

package com.exactpro.cradle.cassandra.amazon.dao;

import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.amazon.dao.messages.StreamsOperator;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.AmazonTestEventMessagesOperator;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.RootTestEventDatesOperator;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;

public class AmazonOperators extends CassandraOperators
{
	private StreamsOperator streamsOperator;
	private RootTestEventDatesOperator rootTestEventDatesOperator;
	private AmazonTestEventMessagesOperator amazonTestEventMessagesOperator;

	public AmazonOperators(CassandraDataMapper cassandraDataMapper, AmazonDataMapper amazonDataMapper, CassandraStorageSettings settings)
	{
		super(cassandraDataMapper, settings);
		streamsOperator = amazonDataMapper.streamsOperator(settings.getKeyspace(), settings.getStreamsTableName());
		rootTestEventDatesOperator = amazonDataMapper.rootTestEventDatesOperator(settings.getKeyspace(), settings.getRootTestEventsDatesTableName());
		amazonTestEventMessagesOperator = amazonDataMapper.amazonTestEventMessagesOperator(settings.getKeyspace(), settings.getTestEventsMessagesTableName());
	}

	public StreamsOperator getStreamsOperator()
	{
		return streamsOperator;
	}

	public RootTestEventDatesOperator getRootTestEventDatesOperator()
	{
		return rootTestEventDatesOperator;
	}

	public AmazonTestEventMessagesOperator getAmazonTestEventMessagesOperator()
	{
		return amazonTestEventMessagesOperator;
	}
}
