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
import com.exactpro.cradle.StoredTestEvent;
import com.exactpro.cradle.StoredTestEventBuilder;
import com.exactpro.cradle.utils.CradleUtils;

public class TestEventUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId)
	{
		return selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
	}
	
	public static StoredTestEvent toTestEvent(Row row) throws TestEventException
	{
		String id = row.getUuid(ID).toString();
		
		UUID parentId = row.getUuid(PARENT_ID),
				reportId = row.getUuid(REPORT_ID);
		
		StoredTestEventBuilder builder = new StoredTestEventBuilder().id(id)
				.name(row.getString(NAME))
				.type(row.getString(TYPE))
				.startTimestamp(row.getInstant(START_TIMESTAMP))
				.endTimestamp(row.getInstant(END_TIMESTAMP))
				.success(row.getBoolean(SUCCESS));
		
		if (parentId != null)
			builder = builder.parent(parentId.toString());
		if (reportId != null)
			builder = builder.report(reportId.toString());
		
		ByteBuffer contentsBuffer = row.getByteBuffer(CONTENT);
		byte[] content = contentsBuffer == null ? new byte[0] : contentsBuffer.array();
		if (row.getBoolean(COMPRESSED))
		{
			try
			{
				content = CradleUtils.decompressData(content);
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
