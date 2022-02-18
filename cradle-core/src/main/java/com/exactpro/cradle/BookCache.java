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
