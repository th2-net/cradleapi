/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class BookInfoMetrics {
    private static final String REQUEST_METHOD_LABEL = "method";
    private static final String BOOK_LABEL = "book";
    private static final String CACHE_NAME_LABEL = "cache";
    private static final String INVALIDATE_CAUSE_LABEL = "cause";
    private static final Counter PAGE_REQUEST_COUNTER = Counter.build()
            .name("cradle_page_cache_page_request_total")
            .help("Page requests number from cache")
            .labelNames(BOOK_LABEL, CACHE_NAME_LABEL, REQUEST_METHOD_LABEL)
            .register();
    private static final Map<PageRequestKey, Counter.Child> PAGE_REQUEST_MAP = new ConcurrentHashMap<>();
    private static final Counter INVALIDATE_CACHE_COUNTER = Counter.build()
            .name("cradle_page_cache_invalidate_total")
            .help("Cache invalidates")
            .labelNames(BOOK_LABEL, CACHE_NAME_LABEL, INVALIDATE_CAUSE_LABEL)
            .register();
    private static final Map<InvalidateKey, Counter.Child> INVALIDATE_CACHE_MAP = new ConcurrentHashMap<>();

    private static final Summary PAGE_LOADS_COUNTER = Summary.build()
            .name("cradle_page_cache_page_loads_total")
            .help("Page loads number to cache")
            .labelNames(BOOK_LABEL, CACHE_NAME_LABEL)
            .register();

    private static final Map<LoadsKey, Summary.Child> PAGE_LOADS_MAP = new ConcurrentHashMap<>();

    public void incRequest(String book, CacheName cacheName, RequestMethod method) {
        if (cacheName == null) {
            return;
        }

        PAGE_REQUEST_MAP.computeIfAbsent(
                new PageRequestKey(book, cacheName, method), key -> PAGE_REQUEST_COUNTER.labels(key.toLabels())
        ).inc();
    }

    public void incInvalidate(String book, CacheName cacheName, RemovalCause cause) {
        INVALIDATE_CACHE_MAP.computeIfAbsent(
                new InvalidateKey(book, cacheName, cause), key -> INVALIDATE_CACHE_COUNTER.labels(key.toLabels())
        ).inc();
    }

    public void incLoads(String book, CacheName cacheName, int value) {
        LoadsKey loadsKey = new LoadsKey(book, cacheName);
        PAGE_LOADS_MAP.computeIfAbsent(
                loadsKey, key -> PAGE_LOADS_COUNTER.labels(key.toLabels())
        ).observe(value);
    }

    public enum RequestMethod {
        GET,
        NEXT,
        PREVIOUS,
        FIND
    }
    public enum CacheName {
        HOT,
        RANDOM
    }

    private static class LoadsKey {
        private final String book;
        private final CacheName cacheName;

        private LoadsKey(String book, CacheName cacheName) {
            this.book = book;
            this.cacheName = cacheName;
        }

        private String[] toLabels() {
            return new String[] {book, cacheName.name()};
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LoadsKey that = (LoadsKey) o;
            return Objects.equals(book, that.book) && cacheName == that.cacheName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(book, cacheName);
        }
    }
    private static class InvalidateKey {
        private final String book;
        private final CacheName cacheName;
        private final RemovalCause cause;

        private InvalidateKey(String book, CacheName cacheName, RemovalCause cause) {
            this.book = book;
            this.cacheName = cacheName;
            this.cause = cause;
        }

        private String[] toLabels() {
            return new String[] {book, cacheName.name(), cause.name()};
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InvalidateKey that = (InvalidateKey) o;
            return Objects.equals(book, that.book) && cacheName == that.cacheName && cause == that.cause;
        }

        @Override
        public int hashCode() {
            return Objects.hash(book, cacheName, cause);
        }
    }

    private static class PageRequestKey {
        private final String book;
        private final CacheName cacheName;
        private final RequestMethod method;

        private PageRequestKey(String book, CacheName cacheName, RequestMethod method) {
            this.book = book;
            this.cacheName = cacheName;
            this.method = method;
        }

        private String[] toLabels() {
            return new String[] {book, cacheName.name(), method.name()};
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PageRequestKey that = (PageRequestKey) o;
            return Objects.equals(book, that.book) && method == that.method && cacheName == that.cacheName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(book, method, cacheName);
        }
    }
}
