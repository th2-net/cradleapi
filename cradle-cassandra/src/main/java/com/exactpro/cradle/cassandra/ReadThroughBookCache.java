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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ReadThroughBookCache implements BookCache {
    private static final Logger logger = LoggerFactory.getLogger(ReadThroughBookCache.class);

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

    public BookInfo getBook (BookId bookId) {
        if (!books.containsKey(bookId)) {
            logger.info("Could not find book named {} in cache, tyring getting from db", bookId.getName());
            try {
                books.put(bookId, loadBook(bookId));
            } catch (Exception e) {
                logger.warn("Could not find book named {} in database", bookId.getName());
            }
        }

        return books.get(bookId);
    }

    public Collection<PageInfo> loadPageInfo(BookId bookId) throws IOException
    {
        Collection<PageInfo> result = new ArrayList<>();
        try
        {
            for (PageEntity pageEntity : ops.getOperators(bookId, readAttrs).getPageOperator().getAll(bookId.getName(), readAttrs))
            {
                if (pageEntity.getRemoved() == null)
                    result.add(pageEntity.toPageInfo());
            }
        }
        catch (Exception e)
        {
            throw new IOException("Error while loading pages of book '"+bookId+"'", e);
        }
        return result;
    }

    public BookInfo loadBook (BookId bookId) throws IOException {
        try {
            BookEntity bookEntity = ops.getCradleBookOperator().get(bookId.getName(), readAttrs);

            if (!bookEntity.getSchemaVersion().equals(schemaVersion)) {
                throw new IOException(String.format("Unsupported schema version for the book \"%s\". Expected: %s, found: %s. Skipping",
                        bookEntity.getName(),
                        schemaVersion,
                        bookEntity.getSchemaVersion()));
            }

            return processBookEntity(bookEntity);
        } catch (Exception e) {
            throw new IOException("Error while loading book", e);
        }
    }

    private BookInfo processBookEntity (BookEntity entity) throws IOException {
        try {
            BookId bookId = new BookId(entity.getName());
            ops.addOperators(bookId, entity.getKeyspaceName());
            Collection<PageInfo> pages = loadPageInfo(bookId);

            return entity.toBookInfo(pages);
        } catch (Exception e) {
            throw new IOException(String.format("Error while loading book \"%s\"", entity.getName()), e);
        }
    }

    public Collection<BookInfo> loadBooks() throws IOException
    {
        Collection<BookInfo> result = new ArrayList<>();
        try
        {
            for (BookEntity bookEntity : ops.getCradleBookOperator().getAll(readAttrs))
            {
                if(!bookEntity.getSchemaVersion().equals(schemaVersion)) {
                    logger.warn("Unsupported schema version for the book \"{}\". Expected: {}, found: {}. Skipping",
                            bookEntity.getName(),
                            schemaVersion,
                            bookEntity.getSchemaVersion()
                    );
                    continue;
                }

                result.add(processBookEntity(bookEntity));
            }
        }
        catch (Exception e)
        {
            throw new IOException("Error while loading books", e);
        }
        return result;
    }

    @Override
    public void updateCachedBook(BookInfo bookInfo) {
        books.put(bookInfo.getId(), bookInfo);
    }

    @Override
    public Collection<BookInfo> getCachedBooks() {
        return books.values();
    }
}
