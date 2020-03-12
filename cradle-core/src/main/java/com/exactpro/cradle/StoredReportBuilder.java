/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle;

import java.time.Instant;

public class StoredReportBuilder
{
	private StoredReport report;
	
	public StoredReportBuilder()
	{
		report = createStoredReport();
	}
	
	
	protected StoredReport createStoredReport()
	{
		return new StoredReport();
	}
	
	
	public StoredReportBuilder id(String id)
	{
		report.setId(id);
		return this;
	}
	
	public StoredReportBuilder name(String name)
	{
		report.setName(name);
		return this;
	}
	
	public StoredReportBuilder timestamp(Instant timestamp)
	{
		report.setTimestamp(timestamp);
		return this;
	}
	
	public StoredReportBuilder success(boolean success)
	{
		report.setSuccess(success);
		return this;
	}
	
	public StoredReportBuilder successful()
	{
		report.setSuccess(true);
		return this;
	}
	
	public StoredReportBuilder failed()
	{
		report.setSuccess(false);
		return this;
	}
	
	public StoredReportBuilder content(byte[] content)
	{
		report.setContent(content);
		return this;
	}
	
	public StoredReport build()
	{
		StoredReport result = report;
		report = createStoredReport();
		return result;
	}
}