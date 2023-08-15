/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;

import java.util.Objects;

@Entity
@CqlName(PageSessionEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class PageSessionEntity {
	public static final String TABLE_NAME = "page_sessions";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_PAGE = "page";
	public static final String FIELD_SESSION_ALIAS = "session_alias";
	public static final String FIELD_DIRECTION = "direction";

	@SuppressWarnings("DefaultAnnotationParam")
	@PartitionKey(0)
	@CqlName(FIELD_BOOK)
	private final String book;

	@PartitionKey(1)
	@CqlName(FIELD_PAGE)
	private final String page;

	@SuppressWarnings("DefaultAnnotationParam")
	@ClusteringColumn(0)
	@CqlName(FIELD_SESSION_ALIAS)
	private final String sessionAlias;

	@ClusteringColumn(1)
	@CqlName(FIELD_DIRECTION)
	private final String direction;

	public PageSessionEntity(String book, String page, String sessionAlias, String direction) {
		this.book = book;
		this.page = page;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
	}

	public PageSessionEntity(StoredMessageId messageId, PageId pageId) {
		this(messageId.getBookId().getName(), pageId.getName(), messageId.getSessionAlias(), messageId.getDirection().getLabel());
	}

	public String getBook() {
		return book;
	}

	public String getPage() {
		return page;
	}

	public String getSessionAlias() {
		return sessionAlias;
	}

	public String getDirection() {
		return direction;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PageSessionEntity)) return false;
		PageSessionEntity that = (PageSessionEntity) o;

		return Objects.equals(getBook(), that.getBook())
				&& Objects.equals(getPage(), that.getPage())
				&& Objects.equals(getSessionAlias(), that.getSessionAlias())
				&& Objects.equals(getDirection(), that.getDirection());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBook(), getPage(), getSessionAlias(), getDirection());
	}
}
