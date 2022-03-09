/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.utils;


import java.util.regex.Pattern;

public class BookPagesNamesChecker {

    // The keyspace name is limited to 48 characters. Books are prefixed with "book_",
    // then the rest should be at most (48 - "book_".length()) = 43 characters
    public static final Pattern BOOK_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]{1,43}$");
    // from 0x20 - 0x7E ascii symbols. Contain all latin character, numbers and printed symbols
    private static final Pattern PAGE_NAME_PATTERN = Pattern.compile("^[\\x20-\\x7E]+$");

    public static final String INVALID_BOOK_NAME_TEXT = "Invalid book name: %s. Up to 43 alphanumeric characters and underscore are allowed.";
    public static final String INVALID_PAGE_NAME_TEXT = "Invalid page name: %s. ASCII printable characters (alphanumeric and symbols) are allowed.";


    public static void validateBookName(String bookName) throws CradleStorageException {
        if (bookName == null || !BOOK_NAME_PATTERN.matcher(bookName).matches()) {
            throw new CradleStorageException(String.format(INVALID_BOOK_NAME_TEXT, bookName));
        }
    }

    public static void validatePageName(String pageName) throws CradleStorageException {
        if (pageName == null || !PAGE_NAME_PATTERN.matcher(pageName).matches()) {
            throw new CradleStorageException(String.format(INVALID_PAGE_NAME_TEXT, pageName));
        }
    }

}
