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
import com.exactpro.cradle.cassandra.dao.CradleOperators;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
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

    private final CradleOperators ops;
    private final Map<BookId, BookInfo> books;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final String schemaVersion;

    public ReadThroughBookCache(CradleOperators ops,
                                Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                                String schemaVersion) {
        this.ops = ops;
        this.books = new ConcurrentHashMap<>();
        this.readAttrs = readAttrs;
        this.schemaVersion = schemaVersion;
    }

    public BookInfo getBook (BookId bookId) throws CradleStorageException {
        if (!books.containsKey(bookId)) {
            logger.info("Book '{}' is absent in cache, trying to get it from DB", bookId.getName());
            try {
                books.put(bookId, loadBook(bookId));
            } catch (CradleStorageException e) {
                logger.error("Could not load book named {}: {}", bookId.getName(), e);
                throw e;
            }
        }

        return books.get(bookId);
    }

    @Override
    public boolean checkBook(BookId bookId) {
        return books.containsKey(bookId);
    }

    public Collection<PageInfo> loadPageInfo(BookId bookId) throws CradleStorageException
    {
        Collection<PageInfo> result = new ArrayList<>();
        for (PageEntity pageEntity : ops.getOperators(bookId).getPageOperator().getAll(bookId.getName(), readAttrs))
        {
            if (pageEntity.getRemoved() == null)
                result.add(pageEntity.toPageInfo());
        }
        return result;
    }

    @Override
    public Collection<PageInfo> loadRemovedPageInfo(BookId bookId) throws CradleStorageException {
        Collection<PageInfo> result = new ArrayList<>();

        for (PageEntity pageEntity : ops.getOperators(bookId).getPageOperator().getAll(bookId.getName(), readAttrs))
        {
            if (pageEntity.getRemoved() != null)
                result.add(pageEntity.toPageInfo());
        }
        return result;
    }

    public BookInfo loadBook (BookId bookId) throws CradleStorageException {
        BookEntity bookEntity = ops.getCradleBookOperator().get(bookId.getName(), readAttrs);

        if (bookEntity == null) {
            throw new CradleStorageException(String.format("Book %s was not found in DB", bookId.getName()));
        }

        if (!bookEntity.getSchemaVersion().equals(schemaVersion)) {
            throw new CradleStorageException(String.format(UNSUPPORTED_SCHEMA_VERSION_FORMAT,
                    bookEntity.getName(),
                    schemaVersion,
                    bookEntity.getSchemaVersion()));
        }

        return processBookEntity(bookEntity);
    }

    private BookInfo processBookEntity (BookEntity entity) throws CradleStorageException {
        BookId bookId = new BookId(entity.getName());
        ops.addOperators(bookId, entity.getKeyspaceName());
        Collection<PageInfo> pages = loadPageInfo(bookId);

        return entity.toBookInfo(pages);
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
