package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.testevents.converters.PageScopeEntityConverter;
import com.exactpro.cradle.cassandra.iterators.ConvertingPagedIterator;
import com.exactpro.cradle.cassandra.resultset.IteratorProvider;
import com.exactpro.cradle.cassandra.retries.SelectQueryExecutor;
import com.exactpro.cradle.counters.Interval;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PageScopesIteratorProvider extends IteratorProvider<String> {


    private final CassandraOperators operators;
    private final ExecutorService composingService;
    private final SelectQueryExecutor selectQueryExecutor;
    private final BookInfo book;
    private final Queue<String> pages;
    private final Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs;

    public PageScopesIteratorProvider(String requestInfo,
                                      CassandraOperators operators,
                                      BookInfo book,
                                      Interval interval,
                                      ExecutorService composingService,
                                      SelectQueryExecutor selectQueryExecutor,
                                      Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs) {
        super(requestInfo);
        this.operators = operators;
        this.book = book;
        this.pages = getPagesInInterval(book, interval);
        this.composingService = composingService;
        this.selectQueryExecutor = selectQueryExecutor;
        this.readAttrs = readAttrs;
    }

    private Queue<String> getPagesInInterval(BookInfo book, Interval interval) {
        Instant start = interval.getStart();
        Instant end = interval.getEnd();

        return book.getPages()
                .stream()
                .filter(page -> (page.getStarted().isAfter(start) && page.getStarted().isBefore(end))
                        || (page.getEnded().isAfter(start) && page.getEnded().isBefore(end)))
                .map(page -> page.getId().getName())
                .collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    public CompletableFuture<Iterator<String>> nextIterator() {
        // All pages have been processed, there can't be next iterator
        if (pages.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        PageScopesOperator pageScopesOperator = operators.getPageScopesOperator();
        PageScopeEntityConverter converter = operators.getPageScopeEntityConverter();

        //TODO handle duplicates
        return pageScopesOperator.getAsync(book.getId().getName(), pages.remove(), readAttrs).thenApplyAsync(rs ->
                new ConvertingPagedIterator<>(rs,
                        selectQueryExecutor,
                        -1,
                        new AtomicInteger(0),
                        PageScopeEntity::getScope,
                        converter::getEntity,
                        getRequestInfo()
                ), composingService);
    }
}
