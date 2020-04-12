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

public class StoredReport
{
	private StoredReportId id;
	private String name;
	private Instant timestamp;
	private boolean success;
	private byte[] content;
	
	public StoredReport()
	{
	}

	public StoredReport(StoredReport copyFrom)
	{
		this.id = copyFrom.getId();
		this.name = copyFrom.getName();
		this.timestamp = copyFrom.getTimestamp();
		this.success = copyFrom.isSuccess();
		this.content = copyFrom.getContent();
	}

	public StoredReportId getId()
	{
		return id;
	}
	
	public void setId(StoredReportId id)
	{
		this.id = id;
	}
	
	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	

	public Instant getTimestamp()
	{
		return timestamp;
	}
	
	public void setTimestamp(Instant timestamp)
	{
		this.timestamp = timestamp;
	}
	
	
	public boolean isSuccess()
	{
		return success;
	}
	
	public void setSuccess(boolean success)
	{
		this.success = success;
	}
	

	public byte[] getContent()
	{
		return content;
	}
	
	public void setContent(byte[] content)
	{
		this.content = content;
	}
}