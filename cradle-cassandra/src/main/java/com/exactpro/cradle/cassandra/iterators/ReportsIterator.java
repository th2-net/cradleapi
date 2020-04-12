/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.iterators;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.cassandra.utils.ReportException;
import com.exactpro.cradle.cassandra.utils.ReportUtils;
import com.exactpro.cradle.reports.StoredReport;

public class ReportsIterator implements Iterator<StoredReport>
{
	private static final Logger logger = LoggerFactory.getLogger(ReportsIterator.class);
	
	private final Iterator<Row> rows;
	
	public ReportsIterator(Iterator<Row> rows)
	{
		this.rows = rows;
	}
	
	@Override
	public boolean hasNext()
	{
		return rows.hasNext();
	}
	
	@Override
	public StoredReport next()
	{
		Row r = rows.next();
		try
		{
			return ReportUtils.toReport(r);
		}
		catch (ReportException e)
		{
			StoredReport result = e.getReport();
			logger.warn("Error while getting report '"+result.getId()+"'. Returned data may be corrupted", e);
			return result;
		}
	}
}