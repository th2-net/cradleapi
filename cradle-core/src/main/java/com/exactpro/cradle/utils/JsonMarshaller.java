/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonMarshaller<T>
{
	private ObjectMapper objectMapper;


	public JsonMarshaller()
	{
		objectMapper = new ObjectMapper();
		configure();
	}

	public JsonMarshaller(JsonFactory jsonFactory)
	{
		objectMapper = new ObjectMapper(jsonFactory);
		configure();
	}
	
	public JsonMarshaller(ObjectMapper objectMapper)
	{
		this.objectMapper = objectMapper;
	}

	protected void configure()
	{
		objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
	}


	public String marshal(T obj) throws IOException
	{
		return objectMapper.writeValueAsString(obj);
	}

	public void marshal(T obj, String destPath) throws IOException
	{
		objectMapper.writeValue(new File(destPath), obj);
	}


	public T unmarshal(String jsonString) throws IOException
	{
		return objectMapper.readValue(jsonString, new TypeReference<T>(){});
	}

	public T unmarshal(String jsonString, Class<T> targetClass) throws IOException
	{
		return objectMapper.readValue(jsonString, targetClass);
	}

	public T unmarshal(File src) throws IOException
	{
		return objectMapper.readValue(src, new TypeReference<T>(){});
	}

	public T unmarshal(File src, Class<T> targetClass) throws IOException
	{
		return objectMapper.readValue(src, targetClass);
	}

	public T unmarshal(Path destPath) throws IOException
	{
		return unmarshal(destPath.toFile());
	}

	public T unmarshal(Path destPath, Class<T> targetClass) throws IOException
	{
		return unmarshal(destPath.toFile(), targetClass);
	}
	
	
	public ObjectMapper getObjectMapper()
	{
		return objectMapper;
	}
}
