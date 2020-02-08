/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.feeder.streams;

import com.exactpro.cradle.CradleStream;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonCradleStream implements CradleStream
{
	private String name, 
		streamData;
	
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
	public String getStreamData()
	{
		return streamData;
	}
	
	public void setStreamData(String streamData)
	{
		this.streamData = streamData;
	}
}
