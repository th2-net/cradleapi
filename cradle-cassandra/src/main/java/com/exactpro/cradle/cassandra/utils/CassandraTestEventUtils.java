/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.utils;

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
