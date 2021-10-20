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

package com.exactpro.cradle.cassandra.dao.books;

import static com.exactpro.cradle.cassandra.StorageConstants.PART;
import static com.exactpro.cradle.cassandra.StorageConstants.START_DATE;
import static com.exactpro.cradle.cassandra.StorageConstants.START_TIME;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;

@Dao
public interface PageOperator
{
	@Select
	PagingIterable<PageEntity> getAll(String book, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query("SELECT * FROM ${qualifiedTableId} WHERE "+PART+"=:part AND ("+START_DATE+", "+START_TIME+")>(:startDate, :startTime)")
	PagingIterable<PageEntity> get(String part, LocalDate startDate, LocalTime startTime, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Update
	ResultSet update(PageEntity entity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	ResultSet write(PageEntity entity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
