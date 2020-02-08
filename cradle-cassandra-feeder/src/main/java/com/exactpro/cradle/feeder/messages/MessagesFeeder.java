/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.feeder.messages;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.feeder.JsonFeeder;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessagesFeeder extends JsonFeeder
{	
	public MessagesFeeder(ObjectMapper jsonMapper, CradleStorage storage)
	{
		super(jsonMapper, storage);
	}

	@Override
	public String feed(String text) throws JsonParseException, JsonMappingException, IOException
	{
		JsonStoredMessage jsonMsg = jsonMapper.readValue(text.getBytes(StandardCharsets.UTF_8), JsonStoredMessage.class);
		return storage.storeMessage(jsonMsg.toStoredMessage()).toString();
	}

}
