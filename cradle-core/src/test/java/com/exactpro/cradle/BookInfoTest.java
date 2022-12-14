package com.exactpro.cradle;

import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;

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

        List<PageInfo> expected = pages.subList(2, 8);
        Collection<PageInfo> actual = bookInfo.getPages(new Interval(bookStart.plus(2, ChronoUnit.HOURS), bookStart.plus(8, ChronoUnit.HOURS).minusMillis(1)));

        Assert.assertEquals(actual, expected);
    }

    @Test(description = "Tests getPages(Interval) when Interval's instants are not exact page starts")
    public void testGetPagesNonExact() {
        try {
            int nOfPages = 10;
            Instant bookStart = Instant.now().minus(nOfPages, ChronoUnit.HOURS);
            List<PageInfo> pages = generateNPages(nOfPages, bookId, bookStart, ChronoUnit.HOURS);
            BookInfo bookInfo = new BookInfo(bookId, null, null, bookStart, pages);

            List<PageInfo> expected = pages.subList(2, 8);
            Collection<PageInfo> actual = bookInfo.getPages(
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

        List<PageInfo> expected = pages.subList(9, 10);
        Collection<PageInfo> actual = bookInfo.getPages(
                new Interval(
                        bookStart.plus(11, ChronoUnit.HOURS),
                        bookStart.plus(12, ChronoUnit.HOURS)));

        Assert.assertEquals(actual, expected);
    }

    @Test(description = "Tests getPages(Interval) when book has some removed pages")
    public void testGetPagesRemovedPages() throws CradleStorageException {
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

        ArrayList<Object> expected = new ArrayList<>();
        expected.add(pages.get(5));
        expected.add(pages.get(7));
        expected.add(pages.get(9));
        Collection<PageInfo> actual = bookInfo.getPages(
                new Interval(
                        bookStart.plus(5, ChronoUnit.HOURS).minus(30, ChronoUnit.MINUTES),
                        bookStart.plus(12, ChronoUnit.HOURS)));

        Assert.assertEquals(actual, expected);
    }

    @Test(description = "Tests getPages(Interval) when book has some removed pages and we're getting pages for empty interval")
    public void testGetPagesRemovedPagesEmptyInterval() throws CradleStorageException {
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

        List<Object> expected = Collections.emptyList();
        Collection<PageInfo> actual = bookInfo.getPages(
                new Interval(
                        bookStart.plus(4, ChronoUnit.HOURS).plus(30, ChronoUnit.MINUTES),
                        bookStart.plus(5, ChronoUnit.HOURS).minus(15, ChronoUnit.MINUTES)));

        Assert.assertEquals(actual, expected);
    }

    @Test(description = "Tests getPages(Interval) when interval starts before pages and ends during pages")
    public void testGetPagesBadIntervalStart() throws CradleStorageException {
        int nOfPages = 10;
        Instant bookStart = Instant.now().minus(nOfPages, ChronoUnit.HOURS);
        List<PageInfo> pages = generateNPages(nOfPages, bookId, bookStart, ChronoUnit.HOURS);
        BookInfo bookInfo = new BookInfo(bookId, null, null, bookStart, pages);

        List<PageInfo> expected = pages.subList(0, 4);
        Collection<PageInfo> actual = bookInfo.getPages(
                new Interval(
                        bookStart.minus(1, ChronoUnit.HOURS),
                        bookStart.plus(3, ChronoUnit.HOURS)));

        Assert.assertEquals(actual, expected);
    }

    @Test(description = "Tests getPages(Interval) when interval starts and ends before pages")
    public void testGetPagesBadInterval() throws CradleStorageException {
        int nOfPages = 10;
        Instant bookStart = Instant.now().minus(nOfPages, ChronoUnit.HOURS);
        List<PageInfo> pages = generateNPages(nOfPages, bookId, bookStart, ChronoUnit.HOURS);
        BookInfo bookInfo = new BookInfo(bookId, null, null, bookStart, pages);

        List<PageInfo> expected = Collections.emptyList();
        Collection<PageInfo> actual = bookInfo.getPages(
                new Interval(
                        bookStart.minus(3, ChronoUnit.HOURS),
                        bookStart.minus(1, ChronoUnit.HOURS)));

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
