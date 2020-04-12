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

import java.io.IOException;

import com.exactpro.cradle.reports.StoredReport;

public class ReportException extends IOException
{
	private static final long serialVersionUID = -7294739795386845145L;
	
	private final StoredReport report;
	
	public ReportException(String message, Throwable cause, StoredReport report)
	{
		super(message, cause);
		this.report = report;
	}
	
	
	public StoredReport getReport()
	{
		return report;
	}
}
