/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.cache;

import java.util.Objects;

public class CachedPageScope {
	private final String book;
    private final String page;
    private final String scope;

    public CachedPageScope(String book, String page, String scope) {
    	this.book = book;
        this.page = page;
        this.scope = scope;
    }

    @Override
    public int hashCode() {
        return Objects.hash(book, page, scope);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CachedPageScope other = (CachedPageScope) obj;
        return Objects.equals(book, other.book) &&
				Objects.equals(page, other.page) &&
				Objects.equals(scope, other.scope);
    }

    @Override
    public String toString() {
        return String.format("%s [book=\"%s\", page=\"%s\", scope=\"%s\"]",
				this.getClass().getSimpleName(), book, page, scope);
    }
}
