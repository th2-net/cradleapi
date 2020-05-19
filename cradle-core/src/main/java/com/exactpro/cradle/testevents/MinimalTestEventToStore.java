/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.testevents;

/**
 * Object to hold minimal information about test event prepared to be stored in Cradle. Events usually extend this class with additional data
 */
public class MinimalTestEventToStore implements MinimalTestEventFields
{
	private StoredTestEventId id;
	private String name,
			type;
	private StoredTestEventId parentId;
	
	
	public static MinimalTestEventToStoreBuilder newMinimalTestEventBuilder()
	{
		return new MinimalTestEventToStoreBuilder();
	}
	
	
	@Override
	public StoredTestEventId getId()
	{
		return id;
	}
	
	public void setId(StoredTestEventId id)
	{
		this.id = id;
	}
	
	
	@Override
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	
	@Override
	public String getType()
	{
		return type;
	}
	
	public void setType(String type)
	{
		this.type = type;
	}
	
	
	@Override
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	public void setParentId(StoredTestEventId parentId)
	{
		this.parentId = parentId;
	}
}
