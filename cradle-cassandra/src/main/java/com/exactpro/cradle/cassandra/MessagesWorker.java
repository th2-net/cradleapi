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

package com.exactpro.cradle.cassandra;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.messages.DetailedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageEntity;
import com.exactpro.cradle.cassandra.dao.messages.TimeMessageOperator;
import com.exactpro.cradle.cassandra.iterators.MessagesIteratorAdapter;
import com.exactpro.cradle.cassandra.iterators.StoredMessageBatchAdapter;
import com.exactpro.cradle.cassandra.utils.CassandraMessageUtils;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.MessageUtils;
import com.exactpro.cradle.utils.TimeUtils;

public class MessagesWorker
{
	private static final Logger logger = LoggerFactory.getLogger(MessagesWorker.class);
	
	private final UUID instanceUuid;
	private final CassandraOperators ops;
	private final CradleObjectsFactory objectsFactory;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			readAttrs;
	private final ExecutorService composingService;
	
	public MessagesWorker(UUID instanceUuid, CassandraOperators ops, CradleObjectsFactory objectsFactory,
			Function<BoundStatementBuilder, BoundStatementBuilder> writeAttrs,
			Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
			ExecutorService composingService)
	{
		this.instanceUuid = instanceUuid;
		this.ops = ops;
		this.objectsFactory = objectsFactory;
		this.writeAttrs = writeAttrs;
		this.readAttrs = readAttrs;
		this.composingService = composingService;
	}
	
	
	public CompletableFuture<?> writeMessageBatch(DetailedMessageBatchEntity entity, StoredMessageBatch batch, boolean rawMessage)
	{
		if (rawMessage)
			return ops.getMessageBatchOperator().writeMessageBatch(entity, writeAttrs)
					.thenComposeAsync(r -> writeTimeMessages(batch.getMessages()), composingService);
		return ops.getProcessedMessageBatchOperator().writeMessageBatch(entity, writeAttrs);
	}
	
	public CompletableFuture<StoredMessage> readMessage(StoredMessageId id, boolean rawMessage)
	{
		return readMessageBatchEntity(id, rawMessage)
				.thenApplyAsync((entity) -> {
					try
					{
						return entity == null ? null : MessageUtils.bytesToOneMessage(entity.getContent(), entity.isCompressed(), id);
					}
					catch (IOException e)
					{
						throw new CompletionException("Error while reading message", e);
					}
				}, composingService);
	}
	
	public CompletableFuture<Collection<StoredMessage>> readMessageBatch(StoredMessageId id)
	{
		return readMessageBatchEntity(id, true)
				.thenApplyAsync(entity -> {
					try
					{
						return entity == null ? null : MessageUtils.bytesToMessages(entity.getContent(), entity.isCompressed());
					}
					catch (IOException e)
					{
						throw new CompletionException("Error while reading message batch", e);
					}
				}, composingService);
	}
	
	public CompletableFuture<Iterable<StoredMessage>> readMessages(StoredMessageFilter filter)
	{
		return readMessageBatchEntities(filter).thenApply(it -> new MessagesIteratorAdapter(filter, it));
	}
	
	public CompletableFuture<Iterable<StoredMessageBatch>> readMessageBatches(StoredMessageFilter filter)
	{
		return readMessageBatchEntities(filter).thenApply(it -> new StoredMessageBatchAdapter(it, objectsFactory, filter == null ? 0 : filter.getLimit()));
	}
	
	
	private CompletableFuture<TimeMessageEntity> writeTimeMessages(Collection<StoredMessage> messages)
	{
		CompletableFuture<TimeMessageEntity> result = CompletableFuture.completedFuture(null);
		Instant ts = null;
		TimeMessageOperator op = ops.getTimeMessageOperator();
		for (StoredMessage msg : messages)
		{
			Instant msgSeconds = TimeUtils.cutNanos(msg.getTimestamp());
			if (msgSeconds.equals(ts))
				continue;
			
			ts = msgSeconds;
			
			TimeMessageEntity timeEntity = new TimeMessageEntity(msg, instanceUuid);
			result = result.thenComposeAsync(r -> {
				logger.trace("Executing time/message storing query for message {}", msg.getId());
				return op.writeMessage(timeEntity, writeAttrs);
			}, composingService);
		}
		return result;
	}
	
	private CompletableFuture<DetailedMessageBatchEntity> readMessageBatchEntity(StoredMessageId messageId, boolean rawMessage)
	{
		MessageBatchOperator op = rawMessage ? ops.getMessageBatchOperator() : ops.getProcessedMessageBatchOperator();
		return CassandraMessageUtils.getMessageBatch(messageId, op, instanceUuid, readAttrs);
	}
	
	private CompletableFuture<MappedAsyncPagingIterable<DetailedMessageBatchEntity>> readMessageBatchEntities(StoredMessageFilter filter)
	{
		MessageBatchOperator op = ops.getMessageBatchOperator();
		return op.filterMessages(instanceUuid, filter, op, readAttrs);
	}
}
