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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.exactpro.cradle.cassandra.dao.messages.TimeMessageEntity;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface TimeMessageOperator
{
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction AND "+MESSAGE_DATE+"=:messageDate AND "
			+MESSAGE_TIME+">=:messageTime ORDER BY "+MESSAGE_TIME+" ASC limit 1")
	CompletableFuture<TimeMessageEntity> getNearestMessageAfter(UUID instanceId, String streamName, LocalDate messageDate, String direction,
			LocalTime messageTime, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "
			+INSTANCE_ID+"=:instanceId AND "+STREAM_NAME+"=:streamName AND "+DIRECTION+"=:direction AND "+MESSAGE_DATE+"=:messageDate AND "
			+MESSAGE_TIME+"<=:messageTime ORDER BY "+MESSAGE_TIME+" DESC limit 1")
	CompletableFuture<TimeMessageEntity> getNearestMessageBefore(UUID instanceId, String streamName, LocalDate messageDate, String direction, 
			LocalTime messageTime, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	CompletableFuture<TimeMessageEntity> writeMessage(TimeMessageEntity timeMessage, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
