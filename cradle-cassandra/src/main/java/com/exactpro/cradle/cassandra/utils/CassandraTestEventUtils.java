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
import java.util.Collection;
import java.util.UUID;
import java.util.zip.DataFormatException;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatchId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.TestEventUtils;

public class CassandraTestEventUtils
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
	
	public static StoredTestEvent toTestEvent(Row row, StoredTestEventId id) throws IOException
	{
		StoredTestEventId parentId = getParentId(row);
		byte[] contentBytes = getTestEventContentBytes(row);
		return TestEventUtils.deserializeOneTestEvent(contentBytes, id, parentId);
	}
	
	public static Collection<StoredTestEvent> toTestEvents(Row row) throws IOException
	{
		StoredTestEventId parentId = getParentId(row);
		byte[] contentBytes = getTestEventContentBytes(row);
		return TestEventUtils.deserializeTestEvents(contentBytes, new StoredTestEventBatchId(row.getString(ID)), 
				parentId);
	}
	
	
	private static byte[] getTestEventContentBytes(Row row) throws IOException
	{
		Boolean compressed = row.get(COMPRESSED, GenericType.BOOLEAN);
		
		ByteBuffer contentByteBuffer = row.get(CONTENT, GenericType.BYTE_BUFFER);
		byte[] contentBytes = contentByteBuffer.array();
		if (!Boolean.TRUE.equals(compressed))
			return contentBytes;
		
		try
		{
			return CompressionUtils.decompressData(contentBytes);
		}
		catch (IOException | DataFormatException e)
		{
			String id = row.getString(ID);
			throw new IOException(String.format("Could not decompress test event batch contents (ID: '%s') from Cradle",	id), e);
		}
	}
	
	private static StoredTestEventId getParentId(Row row) throws IOException
	{
		String parentId = row.getString(PARENT_ID);
		try
		{
			return parentId != null ? StoredTestEventId.fromString(parentId) : null;
		}
		catch (CradleIdException e)
		{
			throw new IOException("Could not parse parent ID", e);
		}
	}
}
