/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.BookCache;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
import com.exactpro.cradle.errors.BookNotFoundException;
import com.exactpro.cradle.utils.CradleStorageException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_PAGE_REMOVE_TIME;
import static com.exactpro.cradle.cassandra.utils.StorageUtils.toLocalDate;
import static com.exactpro.cradle.cassandra.utils.StorageUtils.toLocalTime;

public class ReadThroughBookCache implements BookCache {

    private final CassandraOperators operators;
    private final Map<BookId, BookInfo> books;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;
    private final String schemaVersion;
    private final int raCacheSize;
    private final long raCacheInvalidateInterval;

    public ReadThroughBookCache(CassandraOperators operators,
                                Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs,
                                String schemaVersion, int raCacheSize, long raCacheInvalidateInterval) {
        this.operators = operators;
        this.books = new ConcurrentHashMap<>();
        this.readAttrs = readAttrs;
        this.schemaVersion = schemaVersion;
        this.raCacheSize = raCacheSize;
        this.raCacheInvalidateInterval = raCacheInvalidateInterval;
    }

    public BookInfo getBook(BookId bookId) throws BookNotFoundException {
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

    public Collection<PageInfo> loadPageInfo(BookId bookId, boolean loadRemoved) {
        Collection<PageInfo> result = new ArrayList<>();
        for (PageEntity pageEntity : operators.getPageOperator().getAll(bookId.getName(), readAttrs)) {
            if (loadRemoved || pageEntity.getRemoved() == null || pageEntity.getRemoved().equals(DEFAULT_PAGE_REMOVE_TIME)) {
                result.add(pageEntity.toPageInfo());
            }
        }
        return result;
    }

    public Collection<PageInfo> loadPageInfo(BookId bookId, Instant start, Instant end, boolean loadRemoved) {
        Collection<PageInfo> result = new ArrayList<>();
        LocalDate startDate = start != null ? toLocalDate(start) : LocalDate.MIN;
        LocalTime startTime = start != null ? toLocalTime(start) : LocalTime.MIN;
        LocalDate endDate = end != null ? toLocalDate(end) : LocalDate.MAX;
        LocalTime endTime = end != null ? toLocalTime(end) : LocalTime.MAX;
        for (PageEntity pageEntity : operators.getPageOperator().getByEnd(
                bookId.getName(),
                endDate,
                endTime,
                readAttrs
        )) {
            if (loadRemoved || pageEntity.getRemoved() == null || pageEntity.getRemoved().equals(DEFAULT_PAGE_REMOVE_TIME)) {
                if (pageEntity.getEndDate() == null && pageEntity.getEndTime() == null
                    || startDate.isBefore(pageEntity.getEndDate())
                    || startDate.equals(pageEntity.getEndDate()) && !startTime.isAfter(pageEntity.getEndTime())) {
                    result.add(pageEntity.toPageInfo());
                }
            }
        }
        return result;
    }

    public PageInfo loadLastPageInfo(BookId bookId, boolean loadRemoved) {
        for (PageEntity pageEntity : operators.getPageOperator().getLast(
                bookId.getName(),
                readAttrs
        )) {
            if (loadRemoved || pageEntity.getRemoved() == null || pageEntity.getRemoved().equals(DEFAULT_PAGE_REMOVE_TIME)) {
                return pageEntity.toPageInfo();
            }
        }
        return null;
    }

    public PageInfo loadFirstPageInfo(BookId bookId, boolean loadRemoved) {
        for (PageEntity pageEntity : operators.getPageOperator().getFirst(
                bookId.getName(),
                readAttrs
        )) {
            if (loadRemoved || pageEntity.getRemoved() == null || pageEntity.getRemoved().equals(DEFAULT_PAGE_REMOVE_TIME)) {
                return pageEntity.toPageInfo();
            }
        }
        return null;
    }

    public BookInfo loadBook(BookId bookId) throws CradleStorageException {
        BookEntity bookEntity = operators.getBookOperator().get(bookId.getName(), readAttrs);

        if (bookEntity == null) {
            throw new CradleStorageException(String.format("Book %s was not found in DB", bookId.getName()));
        }

        if (!bookEntity.getSchemaVersion().equals(schemaVersion)) {
            throw new CradleStorageException(String.format("Unsupported schema version for the book \"%s\". Expected: %s, found: %s",
                    bookEntity.getName(),
                    schemaVersion,
                    bookEntity.getSchemaVersion()));
        }

        return toBookInfo(bookEntity);
    }

    private Collection<PageInfo> loadPageInfo(BookId bookId, Instant start, Instant end) {
        return loadPageInfo(bookId, start, end, false);
    }

    private PageInfo loadLastPageInfo(BookId bookId) {
        return  loadLastPageInfo(bookId, false);
    }

    private PageInfo loadFirstPageInfo(BookId bookId) {
        return loadFirstPageInfo(bookId, false);
    }

    private BookInfo toBookInfo(BookEntity entity) throws CradleStorageException {
        try {
            return new BookInfo(
                    new BookId(entity.getName()),
                    entity.getFullName(),
                    entity.getDesc(),
                    entity.getCreated(),
                    raCacheSize,
                    raCacheInvalidateInterval,
                    this::loadPageInfo,
                    this::loadFirstPageInfo,
                    this::loadLastPageInfo);
        } catch (RuntimeException e) {
            throw new CradleStorageException("Inconsistent state of book '"+entity.getName(), e);
        }
    }

    @Override
    public Collection<BookInfo> getCachedBooks() {
        return Collections.unmodifiableCollection(books.values());
    }
}
