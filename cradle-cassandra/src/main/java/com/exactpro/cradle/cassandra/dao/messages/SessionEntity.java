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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.mapper.annotations.*;

import java.util.Objects;

@Entity
@CqlName(SessionEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class SessionEntity {
	public static final String TABLE_NAME = "sessions";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_SESSION_ALIAS = "session_alias";
	@PartitionKey(0)
	@CqlName(FIELD_BOOK)
	private final String book;

	@ClusteringColumn(0)
	@CqlName(FIELD_SESSION_ALIAS)
	private final String sessionAlias;

	public SessionEntity(String book, String sessionAlias) {
		this.book = book;
		this.sessionAlias = sessionAlias;
	}
	
	public String getBook()	{
		return book;
	}
	public String getSessionAlias()	{
		return sessionAlias;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof SessionEntity)) return false;
		SessionEntity that = (SessionEntity) o;

		return Objects.equals(getBook(), that.getBook())
				&& Objects.equals(getSessionAlias(), that.getSessionAlias());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBook(), getSessionAlias());
	}
}
