/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.*;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
import com.exactpro.cradle.exception.BookNotFoundException;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ReadThroughBookCache implements BookCache {
    private static final Logger logger = LoggerFactory.getLogger(ReadThroughBookCache.class);

    private final String UNSUPPORTED_SCHEMA_VERSION_FORMAT = "Unsupported schema version for the book \"%s\". Expected: %s, found: %s";

    private final CassandraOperators operators;
    private final Map<BookId, BookInfo> books;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final String schemaVersion;

    public ReadThroughBookCache(CassandraOperators operators,
                                Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                                String schemaVersion) {
        this.operators = operators;
        this.books = new ConcurrentHashMap<>();
        this.readAttrs = readAttrs;
        this.schemaVersion = schemaVersion;
    }

    public BookInfo getBook (BookId bookId) throws BookNotFoundException {
        try {
            return books.computeIfAbsent(bookId, bookId1 -> {
                try {
                    return loadBook(bookId1);
                } catch (CradleStorageException e) {
                    throw new RuntimeException(String.format("Could not load book named %s", bookId1.getName()), e);
                }
            });
        } catch (RuntimeException e) {
            throw new BookNotFoundException(String.format("Could not load book named %s", bookId.getName()), e);
        }
    }

    @Override
    public boolean checkBook(BookId bookId) {
        return books.containsKey(bookId);
    }

    public Collection<PageInfo> loadPageInfo(BookId bookId, boolean loadRemoved) throws CradleStorageException
    {
        Collection<PageInfo> result = new ArrayList<>();
        for (PageEntity pageEntity : operators.getPageOperator().getAll(bookId.getName(), readAttrs))
        {
            if (loadRemoved || pageEntity.getRemoved() == null) {
                result.add(pageEntity.toPageInfo());
            }
        }
        return result;
    }

    public BookInfo loadBook (BookId bookId) throws CradleStorageException {
        BookEntity bookEntity = operators.getBookOperator().get(bookId.getName(), readAttrs);

        if (bookEntity == null) {
            throw new CradleStorageException(String.format("Book %s was not found in DB", bookId.getName()));
        }

        if (!bookEntity.getSchemaVersion().equals(schemaVersion)) {
            throw new CradleStorageException(String.format(UNSUPPORTED_SCHEMA_VERSION_FORMAT,
                    bookEntity.getName(),
                    schemaVersion,
                    bookEntity.getSchemaVersion()));
        }

        return toBookInfo(bookEntity);
    }

    private BookInfo toBookInfo(BookEntity entity) throws CradleStorageException {
        BookId bookId = new BookId(entity.getName());
        Collection<PageInfo> pages = loadPageInfo(bookId, false);

        return new BookInfo(new BookId(entity.getName()), entity.getFullName(), entity.getDesc(), entity.getCreated(), pages);
    }

    @Override
    public void updateCachedBook(BookInfo bookInfo) {
        books.put(bookInfo.getId(), bookInfo);
    }

    @Override
    public Collection<BookInfo> getCachedBooks() {
        return Collections.unmodifiableCollection(books.values());
    }
}
