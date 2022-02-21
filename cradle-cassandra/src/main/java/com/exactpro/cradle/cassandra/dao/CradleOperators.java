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

package com.exactpro.cradle.cassandra.dao;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.CradleBookOperator;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CradleOperators
{
	private final Logger logger = LoggerFactory.getLogger (CradleOperators.class);

	private final Map<BookId, BookOperators> bookOps;
	private final CassandraDataMapper dataMapper;
	private final CassandraStorageSettings settings;
	private final CradleBookOperator cradleBookOp;
	private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
	
	public CradleOperators(CassandraDataMapper dataMapper, CassandraStorageSettings settings, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs)
	{
		bookOps = new ConcurrentHashMap<>();
		this.dataMapper = dataMapper;
		this.settings = settings;
		this.readAttrs = readAttrs;

		String infoKeyspace = settings.getCradleInfoKeyspace();
		this.cradleBookOp = dataMapper.cradleBookOperator(infoKeyspace, settings.getBooksTable());
	}
	
	public BookOperators getOperators(BookId bookId) throws CradleStorageException
	{
		BookOperators result = bookOps.get(bookId);

		if (result == null) {
			logger.info("{} book was absent in operators cache, trying to get it from DB", bookId);
			BookEntity entity = cradleBookOp.get(bookId.getName(), readAttrs);
			if (entity == null) {
				throw new CradleStorageException("No operators prepared for book '"+bookId+"'");
			}

			addOperators(bookId, entity.getKeyspaceName());

			result = bookOps.get(bookId);
		}
		return result;
	}
	
	public BookOperators addOperators(BookId bookId, String keyspace)
	{
		BookOperators result = new BookOperators(bookId, dataMapper, keyspace, settings);
		bookOps.put(bookId, result);
		return result;
	}
	
	public CradleBookOperator getCradleBookOperator()
	{
		return cradleBookOp;
	}
}
