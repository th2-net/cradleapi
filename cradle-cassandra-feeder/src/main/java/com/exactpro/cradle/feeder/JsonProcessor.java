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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class JsonProcessor
{
	private static final Logger logger = LoggerFactory.getLogger(JsonProcessor.class);
	
	private final String entityName;
	private final JsonFeeder feeder;
	
	private long counter;
	
	public JsonProcessor(String entityName, JsonFeeder feeder)
	{
		this.entityName = entityName;
		this.feeder = feeder;
	}
	
	public void process(File file) throws IOException
	{
		counter = 0;
		logger.info("Storing "+entityName+"...");
		try (BufferedReader reader = new BufferedReader(new FileReader(file)))
		{
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null)
			{
				if (line.isEmpty())
				{
					if (sb.length() > 0)
					{
						processString(sb.toString());
						sb = new StringBuilder();
					}
				}
				
				sb.append(line).append("\r\n");
			}
			
			if (sb.length() > 0)
				processString(sb.toString());
		}
		logger.info("Total stored "+counter+" "+entityName);
	}
	
	
	private void processString(String text) throws JsonParseException, JsonMappingException, IOException
	{
		String id = feeder.feed(text);
		if (++counter % 1000 == 0)
			logger.info("Stored "+counter+" "+entityName+", last stored ID - "+id);
	}
}
