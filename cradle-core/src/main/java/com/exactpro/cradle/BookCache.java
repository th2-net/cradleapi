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
package com.exactpro.cradle;

import java.io.IOException;
import java.util.Collection;

/*
    Interface which should be implemented along with
    CradleStorage's implementations, cache should be used
    for getting books and pages and should be updated
    on write
 */

public interface BookCache {
    BookInfo getBook (BookId bookId);
    Collection<PageInfo> loadPageInfo(BookId bookId) throws IOException;
    BookInfo loadBook (BookId bookId) throws IOException;
    Collection<BookInfo> loadBooks() throws IOException;
    void updateCachedBook (BookInfo bookInfo);
    Collection<BookInfo> getCachedBooks ();
}
