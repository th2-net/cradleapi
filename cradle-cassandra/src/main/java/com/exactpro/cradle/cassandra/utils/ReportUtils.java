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
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.StoredReportBuilder;
import com.exactpro.cradle.utils.CompressionUtils;

public class ReportUtils
{
	public static Select prepareSelect(String keyspace, String tableName, UUID instanceId)
	{
		return selectFrom(keyspace, tableName)
				.all()
				.whereColumn(INSTANCE_ID).isEqualTo(literal(instanceId));
	}
	
	public static StoredReport toReport(Row row) throws ReportException
	{
		String id = row.getString(ID);
		
		StoredReportBuilder builder = new StoredReportBuilder().id(id)
				.name(row.getString(NAME))
				.timestamp(row.getInstant(TIMESTAMP))
				.success(row.getBoolean(SUCCESS));
		
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
				throw new ReportException("Could not decompress report contents (ID: '"+id+"') from global storage",	e,
						builder.build());
			}
		}
		return builder.content(content)
				.build();
	}
}
