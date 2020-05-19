/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.dao.testevents;

import java.util.UUID;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import static com.exactpro.cradle.cassandra.StorageConstants.*;

@Dao
public interface TestEventOperator
{
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+ROOT+" IN (true, false) and "+ID+"=:id")
	TestEventEntity get(UUID instanceId, String id, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+ROOT+"=true")
	PagingIterable<TestEventEntity> getRootEvents(UUID instanceId, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "+INSTANCE_ID+"=:instanceId AND "+ROOT+" IN (true, false) and "+PARENT_ID+"=:parentId ALLOW FILTERING")
	PagingIterable<TestEventEntity> getChildren(UUID instanceId, String parentId, 
			Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	DetailedTestEventEntity write(DetailedTestEventEntity testEvent, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
