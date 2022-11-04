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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.*;

import java.util.Objects;

@Entity
@CqlName(PageScopeEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class PageScopeEntity {
	public static final String TABLE_NAME = "page_scopes";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_PAGE = "page";
	public static final String FIELD_SCOPE = "scope";

	@PartitionKey(0)
	@CqlName(FIELD_BOOK)
	private final String book;

	@PartitionKey(1)
	@CqlName(FIELD_PAGE)
	private final String page;

	@ClusteringColumn(0)
	@CqlName(FIELD_SCOPE)
	private final String scope;
	
	public PageScopeEntity(String book, String page, String scope) {
		this.book = book;
		this.page = page;
		this.scope = scope;
	}

	public String getBook() {
		return book;
	}
	public String getPage() {
		return page;
	}
	public String getScope() {
		return scope;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PageScopeEntity)) return false;
		PageScopeEntity that = (PageScopeEntity) o;

		return Objects.equals(getBook(), that.getBook())
				&& Objects.equals(getPage(), that.getPage())
				&& Objects.equals(getScope(), that.getScope());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBook(), getPage(), getScope());
	}
}
