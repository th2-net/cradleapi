/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
