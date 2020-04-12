/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.reports;

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
	
	private void initIfNeeded()
	{
		if (report == null)
			report = createStoredReport();
	}
	
	
	public StoredReportBuilder id(StoredReportId id)
	{
		initIfNeeded();
		report.setId(id);
		return this;
	}
	
	public StoredReportBuilder name(String name)
	{
		initIfNeeded();
		report.setName(name);
		return this;
	}
	
	public StoredReportBuilder timestamp(Instant timestamp)
	{
		initIfNeeded();
		report.setTimestamp(timestamp);
		return this;
	}
	
	public StoredReportBuilder success(boolean success)
	{
		initIfNeeded();
		report.setSuccess(success);
		return this;
	}
	
	public StoredReportBuilder successful()
	{
		initIfNeeded();
		report.setSuccess(true);
		return this;
	}
	
	public StoredReportBuilder failed()
	{
		initIfNeeded();
		report.setSuccess(false);
		return this;
	}
	
	public StoredReportBuilder content(byte[] content)
	{
		initIfNeeded();
		report.setContent(content);
		return this;
	}
	
	public StoredReport build()
	{
		initIfNeeded();
		StoredReport result = report;
		report = null;
		return result;
	}
}