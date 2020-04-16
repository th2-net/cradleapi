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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.DataFormatException;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBuilder;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CompressionUtils;

public class TestEventUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId, boolean onlyRootEvents)
	{
		Select select = selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
		if (onlyRootEvents)
			select = select.whereColumn(IS_ROOT).isEqualTo(literal(true));
		else
			select = select.whereColumn(IS_ROOT).in(literal(true), literal(false));
		return select;
	}
	
	public static StoredTestEvent toTestEvent(Row row) throws TestEventException
	{
		StoredTestEventId id = new StoredTestEventId(row.getString(ID));
		String parentId = row.getString(PARENT_ID);
		
		StoredTestEventBuilder builder = new StoredTestEventBuilder().id(id)
				.name(row.getString(NAME))
				.type(row.getString(TYPE))
				.startTimestamp(row.getInstant(START_TIMESTAMP))
				.endTimestamp(row.getInstant(END_TIMESTAMP))
				.success(row.getBoolean(SUCCESS));
		
		if (parentId != null)
			builder = builder.parent(new StoredTestEventId(parentId));
		
		ByteBuffer contentsBuffer = row.getByteBuffer(CONTENT);
		byte[] content = contentsBuffer == null ? new byte[0] : contentsBuffer.array();
		if (row.getBoolean(COMPRESSED))
		{
			try
			{
				content = CompressionUtils.decompressData(content);
			}
			catch (IOException | DataFormatException e)
			{
				throw new TestEventException("Could not decompress test event contents (ID: '"+id+"') from global storage", e, 
						builder.build());
			}
		}
		
		return builder.content(content)
				.build();
	}
}
