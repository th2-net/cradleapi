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

public class StoredTestEvent
{
	private String id;
	private String name,
			type;
	private Instant startTimestamp,
		endTimestamp;
	private boolean success;
	private byte[] content;
	private String parentId,
		reportId;
	
	public StoredTestEvent()
	{
	}
	
	public StoredTestEvent(StoredTestEvent copyFrom)
	{
		this.id = copyFrom.getId();
		this.name = copyFrom.getName();
		this.type = copyFrom.getType();
		this.startTimestamp = copyFrom.getStartTimestamp();
		this.endTimestamp = copyFrom.endTimestamp;
		this.success = copyFrom.isSuccess();
		this.content = copyFrom.getContent();
		this.parentId = copyFrom.getParentId();
		this.reportId = copyFrom.getReportId();
	}
	
	public String getId()
	{
		return id;
	}
	
	public void setId(String id)
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
	
	
	public String getType()
	{
		return type;
	}
	
	public void setType(String type)
	{
		this.type = type;
	}
	
	
	public Instant getStartTimestamp()
	{
		return startTimestamp;
	}
	
	public void setStartTimestamp(Instant startTimestamp)
	{
		this.startTimestamp = startTimestamp;
	}
	
	
	public Instant getEndTimestamp()
	{
		return endTimestamp;
	}
	
	public void setEndTimestamp(Instant endTimestamp)
	{
		this.endTimestamp = endTimestamp;
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
	
	
	public String getParentId()
	{
		return parentId;
	}
	
	public void setParentId(String parentId)
	{
		this.parentId = parentId;
	}
	
	
	public String getReportId()
	{
		return reportId;
	}
	
	public void setReportId(String reportId)
	{
		this.reportId = reportId;
	}
}