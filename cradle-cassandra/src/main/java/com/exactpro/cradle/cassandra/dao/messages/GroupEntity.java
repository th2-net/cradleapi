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
@CqlName(GroupEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class GroupEntity {
	public static final String TABLE_NAME = "groups";

	public static final String FIELD_BOOK = "book";
	public static final String FIELD_GROUP = "group";
	@PartitionKey(0)
	@CqlName(FIELD_BOOK)
	private final String book;

	@ClusteringColumn(0)
	@CqlName(FIELD_GROUP)
	private final String group;

	public GroupEntity(String book, String group) {
		this.book = book;
		this.group = group;
	}
	
	public String getBook()	{
		return book;
	}
	
	public String getGroup() {
		return group;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof GroupEntity)) return false;
		GroupEntity entity = (GroupEntity) o;

		return Objects.equals(getBook(), entity.getBook()) && Objects.equals(getGroup(), entity.getGroup());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBook(), getGroup());
	}
}
