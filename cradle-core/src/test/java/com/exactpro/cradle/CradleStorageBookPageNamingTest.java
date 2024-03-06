/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle;

import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class CradleStorageBookPageNamingTest {

    private final BookId BOOK_ID = new BookId("Book1");
    private final Instant START_TIMESTAMP = Instant.now();

    private CradleStorage storage;

    @BeforeMethod
    public void prepare() throws CradleStorageException, IOException
    {
        storage = new InMemoryCradleStorage();
        storage.init(false);
        storage.addBook(new BookToAdd(BOOK_ID.getName(), START_TIMESTAMP));
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid book name.*")
    public void invalidBookName() throws IOException, CradleStorageException
    {
        storage.addBook(new BookToAdd("book%%"));
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid book name.*")
    public void invalidBookName2() throws IOException, CradleStorageException
    {
        storage.addBook(new BookToAdd(null));
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid book name.*")
    public void invalidBookName4() throws IOException, CradleStorageException
    {
        storage.addBook(new BookToAdd("bo ok"));
    }

    @Test
    public void validBookName() throws IOException, CradleStorageException
    {
        storage.addBook(new BookToAdd("_"));
        storage.addBook(new BookToAdd("4"));
        storage.addBook(new BookToAdd("book"));
        storage.addBook(new BookToAdd("book_2"));
        storage.addBook(new BookToAdd("BOOK3"));
        storage.addBook(new BookToAdd("4book"));
    }



    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
    public void invalidPageName() throws IOException, CradleStorageException
    {
        storage.addPage(BOOK_ID, "\u0000", Instant.now(), "comment");
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
    public void invalidPageName2() throws IOException, CradleStorageException
    {
        storage.addPage(BOOK_ID, "pa\r\nge1", Instant.now(), "comment");
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
    public void invalidPageName3() throws IOException, CradleStorageException
    {
        storage.addPage(BOOK_ID, "page\u007F", Instant.now(), "comment");
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
    public void invalidPageName4() throws IOException, CradleStorageException
    {
        storage.addPage(BOOK_ID, "page\u0080", Instant.now(), "comment");
    }

    @Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Invalid page name.*")
    public void invalidPageName5() throws IOException, CradleStorageException
    {
        storage.addPage(BOOK_ID, null, Instant.now(), "comment");
    }

    @Test
    public void validPageName() throws IOException, CradleStorageException
    {
        Instant now = Instant.now();
        storage.addPage(BOOK_ID, "pag-~e", now, "comment");
        storage.addPage(BOOK_ID, "pa`ge 2_", now.plus(1, ChronoUnit.HOURS), "comment");
        storage.addPage(BOOK_ID, "'page=_3", now.plus(2, ChronoUnit.HOURS), "comment");
        storage.addPage(BOOK_ID, "4\"pa++ge", now.plus(3, ChronoUnit.HOURS), "comment");
        storage.addPage(BOOK_ID, "pag%%e1", now.plus(4, ChronoUnit.HOURS), "comment");
    }
}
