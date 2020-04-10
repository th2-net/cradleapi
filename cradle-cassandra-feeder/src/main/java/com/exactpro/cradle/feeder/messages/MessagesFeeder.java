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
import java.util.UUID;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.feeder.JsonFeeder;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.utils.CradleStorageException;
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
	public String feed(String text) throws JsonParseException, JsonMappingException, IOException, CradleStorageException
	{
		JsonStoredMessage jsonMsg = jsonMapper.readValue(text.getBytes(StandardCharsets.UTF_8), JsonStoredMessage.class);
		
		StoredMessageBatch batch = new StoredMessageBatch(new StoredMessageBatchId(UUID.randomUUID().toString()));
		batch.addMessage(jsonMsg.toStoredMessage());
		return batch.getId().toString();
	}
}
