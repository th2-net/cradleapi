/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.cradle.cassandra.dao.books.PageEntity.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.*;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;

//TODO: implement getFirstPage / getLastPage methods

@Dao
public interface PageOperator {
	@Select
	PagingIterable<PageEntity> getAll(String book, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query( "SELECT * FROM ${qualifiedTableId} " +
			"WHERE " +
				FIELD_BOOK +"=:book AND " +
			    "(" + FIELD_START_DATE + ", " + FIELD_START_TIME + ") > (:startDate, :startTime)")
	PagingIterable<PageEntity> get(String book, LocalDate startDate, LocalTime startTime,
								   Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT * FROM ${qualifiedTableId} " +
			"WHERE " +
			FIELD_BOOK +"=:book AND " +
			"(" + FIELD_START_DATE + ", " + FIELD_START_TIME + ") >= (:startDate, :startTime) " +
			"AND " +
			"(" + FIELD_START_DATE + ", " + FIELD_START_TIME + ") <= (:endDate, :endTime)")
	PagingIterable<PageEntity> get(String book, LocalDate startDate, LocalTime startTime,
								   LocalDate endDate, LocalTime endTime,
								   Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Update(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
	ResultSet update(PageEntity entity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Insert
	ResultSet write(PageEntity entity, Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
	
	@Query( "UPDATE ${qualifiedTableId} " +
			"SET " +
				FIELD_REMOVED + "=:removed " +
			"WHERE " +
				FIELD_BOOK +"=:book AND " +
				FIELD_START_DATE + "=:startDate AND " +
				FIELD_START_TIME + "=:startTime")
	ResultSet setRemovedStatus(String book, LocalDate startDate, LocalTime startTime, Instant removed,
							   Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Delete(entityClass = PageEntity.class)
	ResultSet remove (String book, LocalDate startDate, LocalTime startTime,
					  Function<BoundStatementBuilder, BoundStatementBuilder> attributes);

	@Query("SELECT * FROM ${qualifiedTableId} " +
			"WHERE " +
			FIELD_BOOK +"=:book AND " +
			"(" + FIELD_START_DATE + ", " + FIELD_START_TIME + ") <= (:startDate, :startTime) "
			+
			"ORDER BY " +
			FIELD_START_DATE + " DESC, " +
			FIELD_START_TIME + " DESC " +
			"LIMIT 1")
	PagingIterable<PageEntity> getPageForLessOrEqual(String book, LocalDate startDate, LocalTime startTime,
													 Function<BoundStatementBuilder, BoundStatementBuilder> attributes);


	@Query("SELECT * FROM ${qualifiedTableId} " +
			"WHERE " +
			FIELD_BOOK +"=:book AND " +
			"(" + FIELD_START_DATE + ", " + FIELD_START_TIME + ") >= (:startDate, :startTime) " +
			"AND " +
			"(" + FIELD_START_DATE + ", " + FIELD_START_TIME + ") <= (:endDate, :endTime)")
	CompletableFuture<MappedAsyncPagingIterable<PageEntity>> getPagesForInterval(String book, LocalDate startDate, LocalTime startTime,
																				LocalDate endDate, LocalTime endTime,
																				Function<BoundStatementBuilder, BoundStatementBuilder> attributes);
}
