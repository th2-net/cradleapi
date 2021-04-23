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

package com.exactpro.cradle.cassandra.amazon;

import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.amazon.dao.AmazonDataMapper;
import com.exactpro.cradle.cassandra.amazon.dao.AmazonDataMapperBuilder;
import com.exactpro.cradle.cassandra.amazon.dao.AmazonOperators;
import com.exactpro.cradle.cassandra.amazon.dao.linkers.AmazonTestEventsMessagesLinker;
import com.exactpro.cradle.cassandra.amazon.dao.messages.AmazonStreamsOperator;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.AmazonTestEventMessagesEntity;
import com.exactpro.cradle.cassandra.amazon.dao.testevents.AmazonTestEventMessagesOperator;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.dao.AsyncOperator;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapper;
import com.exactpro.cradle.cassandra.dao.CassandraDataMapperBuilder;
import com.exactpro.cradle.cassandra.dao.messages.StreamEntity;
import com.exactpro.cradle.cassandra.dao.testevents.RootTestEventDateEntity;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.TEST_EVENTS_MSGS_LINK_MAX_MSGS;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

public class AmazonCradleStorage extends CassandraCradleStorage
{
	private Logger logger = LoggerFactory.getLogger(AmazonCradleStorage.class);
	
	public AmazonCradleStorage(CassandraConnection connection, CassandraStorageSettings settings)
	{
		super(connection, settings);
	}

	@Override
	protected AmazonOperators createOperators()
	{
		CassandraDataMapper cassandraDataMapper = new CassandraDataMapperBuilder(connection.getSession()).build();
		AmazonDataMapper amazonDataMapper = new AmazonDataMapperBuilder(connection.getSession()).build();
		return new AmazonOperators(cassandraDataMapper, amazonDataMapper, settings);
	}

	@Override
	protected AmazonTestEventsMessagesLinker createTestEvetsMessagesLinker()
	{
		return new AmazonTestEventsMessagesLinker(getOps(), instanceUuid, readAttrs, semaphore);
	}

	@Override
	protected void createTables() throws IOException
	{
		new AmazonTablesCreator(exec, settings).createAll();
	}

	@Override
	protected CompletableFuture<Void> writeMessage(StoredMessageBatch batch, boolean rawMessage)
	{
		CompletableFuture<Void> writeMessagefuture = super.writeMessage(batch, rawMessage);
		CompletableFuture<StreamEntity> writeStreamfuture = new AsyncOperator<StreamEntity>(semaphore)
				.getFuture(() -> {
					logger.trace("Executing stream storing query");
					AmazonStreamsOperator op = getOps().getAmazonStreamsOperator();
					StreamEntity stream = new StreamEntity(instanceUuid, batch.getStreamName());
					return op.writeStream(stream, writeAttrs);
				});

		return CompletableFuture.allOf(writeMessagefuture, writeStreamfuture);
	}

	@Override
	protected Collection<CompletableFuture<Void>> doStoreRootTestEvent(StoredTestEvent event)
	{
		Collection<CompletableFuture<Void>> result = new ArrayList<>(super.doStoreRootTestEvent(event));
		result.add(storeRootEventDate(event.getStartTimestamp()).thenAccept(r -> {}));
		return result;
	}

	protected CompletableFuture<RootTestEventDateEntity> storeRootEventDate(Instant startTimestamp)
	{
		return new AsyncOperator<RootTestEventDateEntity>(semaphore).getFuture(() ->
		{
			LocalDate ldt = startTimestamp == null ? null
					: LocalDateTime.ofInstant(startTimestamp, CassandraCradleStorage.TIMEZONE_OFFSET).toLocalDate();
			RootTestEventDateEntity entity = new RootTestEventDateEntity(instanceUuid, ldt);

			logger.trace("Executing root event date storing query");
			return getOps().getAmazonRootTestEventDatesOperator().writeTestEventDate(entity, writeAttrs);
		});
	}

	@Override
	protected CompletableFuture<Void> storeMessagesOfTestEvent(String eventId, List<String> messageIds)
	{
		List<CompletableFuture<AmazonTestEventMessagesEntity>> futures = new ArrayList<>();
		AmazonTestEventMessagesOperator op = getOps().getAmazonTestEventMessagesOperator();
		int msgsSize = messageIds.size();
		for (int left = 0; left < msgsSize; left++)
		{
			int right = min(left + TEST_EVENTS_MSGS_LINK_MAX_MSGS, msgsSize);
			Set<String> curMsgsIds = new HashSet<>(messageIds.subList(left, right));
			logger.trace("Linking {} message(s) to test event {}", curMsgsIds.size(), eventId);

			AmazonTestEventMessagesEntity entity = new AmazonTestEventMessagesEntity();
			entity.setInstanceId(getInstanceUuid());
			entity.setEventId(eventId);
			entity.setMessageIds(curMsgsIds);

			futures.add(new AsyncOperator<AmazonTestEventMessagesEntity>(semaphore)
					.getFuture(() -> op.writeMessages(entity, writeAttrs)));

			left = right - 1;
		}
		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
	}

	@Override
	protected Collection<Instant> doGetRootTestEventsDates() throws IOException
	{
		List<Instant> result = new ArrayList<>();
		for (RootTestEventDateEntity entity : getOps().getAmazonRootTestEventDatesOperator().getDates(readAttrs))
		{
			if (instanceUuid.equals(entity.getInstanceId()))
				result.add(entity.getStartDate().atStartOfDay(TIMEZONE_OFFSET).toInstant());
		}
		result.sort(null);
		return result;
	}

	@Override
	protected Collection<String> doGetStreams() throws IOException
	{
		return Streams.stream(getOps().getAmazonStreamsOperator().getStreams(instanceUuid, readAttrs))
				.map(StreamEntity::getStreamName)
				.sorted()
				.collect(toList());
	}

	@Override
	public AmazonOperators getOps()
	{
		return (AmazonOperators) super.getOps();
	}
}
