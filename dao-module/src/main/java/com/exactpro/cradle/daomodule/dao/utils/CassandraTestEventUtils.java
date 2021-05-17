/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.daomodule.dao.utils;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.exactpro.cradle.cassandra.StorageConstants.*;

import java.util.UUID;

import com.datastax.oss.driver.api.querybuilder.select.Select;

public class CassandraTestEventUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId, boolean onlyRootEvents)
	{
		Select select = selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
		if (onlyRootEvents)
			select = select.whereColumn(ROOT).isEqualTo(literal(true));
		else
			select = select.whereColumn(ROOT).in(literal(true), literal(false));
		return select;
	}
}
