/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.feeder;

import java.io.IOException;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.utils.CradleStorageException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class JsonFeeder
{
	protected final ObjectMapper jsonMapper;
	protected final CradleStorage storage;
	
	public JsonFeeder(ObjectMapper jsonMapper, CradleStorage storage)
	{
		this.jsonMapper = jsonMapper;
		this.storage = storage;
	}
	
	public abstract String feed(String text) throws JsonParseException, JsonMappingException, IOException, CradleStorageException;
}
