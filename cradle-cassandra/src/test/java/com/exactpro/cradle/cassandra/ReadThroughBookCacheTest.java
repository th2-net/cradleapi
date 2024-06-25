/*
 *  Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.BookOperator;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
import com.exactpro.cradle.cassandra.dao.books.PageOperator;
import com.exactpro.cradle.errors.BookNotFoundException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_PAGE_REMOVE_TIME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class ReadThroughBookCacheTest {
    private final Instant currentTime = Instant.now();
    private final ZoneId zoneId = ZoneOffset.UTC;
    private final BookId bookId = new BookId("test-book");
    private final String schemaVersion = "test-schema-version";
    private final BookEntity bookEntity = new BookEntity(bookId.getName(),
            "test-book-full-name",
            "test-description",
            currentTime,
            schemaVersion);
    private final PagingIterable<PageEntity> pagingIterable = mock(PagingIterable.class);
    private final PageOperator pageOperator = mock(PageOperator.class);
    private final BookOperator bookOperator = mock(BookOperator.class);
    private final CassandraOperators operators = mock(CassandraOperators.class);
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs = mock(Function.class);
    private final ReadThroughBookCache cache = new ReadThroughBookCache(operators, readAttrs, schemaVersion, 1, Long.MAX_VALUE);

    List<PageEntity> previousFormatPages = List.of(
            new PageEntity(bookId.getName(),
                    LocalDate.ofInstant(currentTime.minusSeconds(1), zoneId),
                    LocalTime.ofInstant(currentTime.minusSeconds(1), zoneId),
                    "test-page-1",
                    null,
                    LocalDate.ofInstant(currentTime, zoneId),
                    LocalTime.ofInstant(currentTime, zoneId),
                    null,
                    null),
            new PageEntity(bookId.getName(),
                    LocalDate.ofInstant(currentTime, zoneId),
                    LocalTime.ofInstant(currentTime, zoneId),
                    "test-page-2",
                    null,
                    null,
                    null,
                    null,
                    null)
    );
    List<PageEntity> newFormatPages = List.of(
            new PageEntity(bookId.getName(),
                    LocalDate.ofInstant(currentTime.minusSeconds(1), zoneId),
                    LocalTime.ofInstant(currentTime.minusSeconds(1), zoneId),
                    "test-page-1",
                    null,
                    LocalDate.ofInstant(currentTime, zoneId),
                    LocalTime.ofInstant(currentTime, zoneId),
                    null,
                    DEFAULT_PAGE_REMOVE_TIME),
            new PageEntity(bookId.getName(),
                    LocalDate.ofInstant(currentTime, zoneId),
                    LocalTime.ofInstant(currentTime, zoneId),
                    "test-page-2",
                    null,
                    null,
                    null,
                    null,
                    DEFAULT_PAGE_REMOVE_TIME)
    );

    @BeforeMethod
    public void beforeMethod() {
        doReturn(pageOperator).when(operators).getPageOperator();
        doReturn(bookOperator).when(operators).getBookOperator();
        doReturn(pagingIterable).when(pageOperator).getAll(same(bookId.getName()), same(readAttrs));
        doAnswer(invocation -> {
            LocalDate startDate = invocation.getArgument(1);
            LocalTime startTime = invocation.getArgument(2);
            LocalDate endDate = invocation.getArgument(3);
            LocalTime endTime = invocation.getArgument(4);

            List<PageEntity> result = new ArrayList<>();
            for (PageEntity pageEntity : pagingIterable) {
                if ((startDate == null || !startDate.isAfter(pageEntity.getStartDate())) &&
                        (startTime == null || !startTime.isAfter(pageEntity.getStartTime())) &&
                        (endDate == null || pageEntity.getEndDate() == null || !endDate.isBefore(pageEntity.getEndDate())) &&
                        (endTime == null || pageEntity.getEndTime() == null || !endTime.isBefore(pageEntity.getEndTime()))) {
                    result.add(pageEntity);
                }
            }
            PagingIterable<PageEntity> iterable = mock(PagingIterable.class);
            doReturn(result.iterator()).when(iterable).iterator();
            return iterable;
        }).when(pageOperator).getByEnd(same(bookId.getName()), any(), any(), same(readAttrs));
        doReturn(bookEntity).when(bookOperator).get(same(bookId.getName()), same(readAttrs));
    }

    @Test(dataProvider = "loadRemoved")
    public void testLoadPageInfoPreviousFormat(boolean loadRemoved) {
        doReturn(previousFormatPages.iterator()).when(pagingIterable).iterator();

        assertEquals(cache.loadPageInfo(bookId, loadRemoved).size(), 2);
    }

    @Test(dataProvider = "loadRemoved")
    public void testLoadPageInfoNewFormat(boolean loadRemoved) {
        doReturn(newFormatPages.iterator()).when(pagingIterable).iterator();

        assertEquals(cache.loadPageInfo(bookId, loadRemoved).size(), 2);
    }

    @Test
    public void testGetBookPreviousFormat() throws BookNotFoundException {
        doReturn(previousFormatPages.iterator()).when(pagingIterable).iterator();

        assertEquals(cache. getBook(bookId).getId(), bookId);
    }

    @Test
    public void testGetBookNewFormat() throws BookNotFoundException {
        doReturn(newFormatPages.iterator()).when(pagingIterable).iterator();

        assertEquals(cache.getBook(bookId).getId(), bookId);
    }

    @DataProvider(name = "loadRemoved")
    public static Object[][] loadRemovedProvider() {
        return new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}};
    }
}