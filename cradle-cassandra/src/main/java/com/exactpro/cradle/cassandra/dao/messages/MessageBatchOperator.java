/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.dao.messages;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.exactpro.cradle.messages.StoredMessageFilter;
import com.exactpro.cradle.utils.CradleStorageException;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface MessageBatchOperator
{
	@Select
	PagingIterable<MessageBatchEntity> get(UUID instanceId, String streamName, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Select
	PagingIterable<MessageBatchEntity> get(UUID instanceId, String streamName, String direction, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Select
	MessageBatchEntity get(UUID instanceId, String streamName, String direction, long messageIndex, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction AND "
			+MESSAGE_INDEX+">=:fromIndex AND "+MESSAGE_INDEX+"<=:toIndex")
	PagingIterable<MessageBatchEntity> getMessageBatches(UUID instanceId, String streamName, String direction, long fromIndex, long toIndex, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Select
	PagingIterable<MessageBatchEntity> getAll(Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT MAX("+LAST_MESSAGE_INDEX+") FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction ALLOW FILTERING")
	long getLastIndex(UUID instanceId, String streamName, String direction, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@QueryProvider(providerClass = MessageBatchQueryProvider.class, entityHelpers = MessageBatchEntity.class)
	PagingIterable<MessageBatchEntity> filterMessages(UUID instanceId, StoredMessageFilter filter, MessageBatchOperator operator,
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes) throws CradleStorageException;
	
	@Insert
	DetailedMessageBatchEntity write(DetailedMessageBatchEntity message, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
