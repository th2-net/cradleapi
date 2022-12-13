package com.exactpro.cradle;

import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BookInfoTest {

    private BookId bookId;

    @BeforeClass
    public void prepare () {
        bookId = new BookId("book");
    }

    @Test(description = "Tests getPages(Interval) when Interval's instants are exact page starts")
    public void testGetPagesExact() throws CradleStorageException {
        int nOfPages = 10;
        Instant bookStart = Instant.now().minus(nOfPages, ChronoUnit.HOURS);
        List<PageInfo> pages = generateNPages(nOfPages, bookId, bookStart, ChronoUnit.HOURS);
        BookInfo bookInfo = new BookInfo(bookId, null, null, bookStart, pages);

        var expected = pages.subList(2, 8);
        var actual = bookInfo.getPages(new Interval(bookStart.plus(2, ChronoUnit.HOURS), bookStart.plus(8, ChronoUnit.HOURS).minusMillis(1)));

        Assert.assertEquals(actual, expected);
    }

    @Test(description = "Tests getPages(Interval) when Interval's instants are not exact page starts")
    public void testGetPagesNonExact() {
        try {
            int nOfPages = 10;
            Instant bookStart = Instant.now().minus(nOfPages, ChronoUnit.HOURS);
            List<PageInfo> pages = generateNPages(nOfPages, bookId, bookStart, ChronoUnit.HOURS);
            BookInfo bookInfo = new BookInfo(bookId, null, null, bookStart, pages);

            var expected = pages.subList(2, 8);
            var actual = bookInfo.getPages(
                    new Interval(
                            bookStart.plus(2, ChronoUnit.HOURS).plus(30, ChronoUnit.MINUTES),
                            bookStart.plus(8, ChronoUnit.HOURS).minus(30, ChronoUnit.MINUTES)));

            Assert.assertEquals(actual, expected);
        } catch (CradleStorageException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(description = "Tests getPages(Interval) when method should return only last page (interval is in future)")
    public void testGetPagesLastPage() throws CradleStorageException {
        int nOfPages = 10;
        Instant bookStart = Instant.now().minus(nOfPages, ChronoUnit.HOURS);
        List<PageInfo> pages = generateNPages(nOfPages, bookId, bookStart, ChronoUnit.HOURS);
        BookInfo bookInfo = new BookInfo(bookId, null, null, bookStart, pages);

        var expected = pages.subList(9, 10);
        var actual = bookInfo.getPages(
                new Interval(
                        bookStart.plus(11, ChronoUnit.HOURS),
                        bookStart.plus(12, ChronoUnit.HOURS)));

        Assert.assertEquals(actual, expected);
    }

    @Test(description = "Tests getPages(Interval) when book has missing pages")
    public void testGetPagesMissingPages() throws CradleStorageException {
        int nOfPages = 10;
        Instant bookStart = Instant.now().minus(nOfPages, ChronoUnit.HOURS);
        List<PageInfo> pages = generateNPages(nOfPages, bookId, bookStart, ChronoUnit.HOURS);
        List<PageInfo> bookPages = new ArrayList<>(Arrays.asList(
                pages.get(0),
                pages.get(1),
                pages.get(2),
                pages.get(5),
                pages.get(7),
                pages.get(9)));

        BookInfo bookInfo = new BookInfo(bookId, null, null, bookStart, bookPages);

        var expected = new ArrayList<>();
        expected.add(pages.get(5));
        expected.add(pages.get(7));
        expected.add(pages.get(9));
        var actual = bookInfo.getPages(
                new Interval(
                        bookStart.plus(5, ChronoUnit.HOURS).minus(30, ChronoUnit.MINUTES),
                        bookStart.plus(12, ChronoUnit.HOURS)));

        Assert.assertEquals(actual, expected);
    }

    private List<PageInfo> generateNPages (int nOfPages, BookId bookId, Instant start, TemporalUnit pageDuration) {
        List<PageInfo> pages = new ArrayList<>();
        for (int i = 0; i < nOfPages-1; i ++) {
            PageInfo pageInfo = new PageInfo(
                    new PageId(bookId, "PageId-" + i),
                    start.plus(i, pageDuration),
                    start.plus(i+1, pageDuration),
                    "",
                    null,
                    null);
            pages.add(pageInfo);
        }
        PageInfo lastPageInfo = new PageInfo(
                new PageId(bookId, "PageId-" + (nOfPages-1)),
                start.plus(nOfPages-1, pageDuration),
                null,
                "",
                null,
                null);
        pages.add(lastPageInfo);

        return pages;
    }
}
