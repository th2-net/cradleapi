/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.serialization;

import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.BatchedStoredTestEventMetadata;
import org.apache.commons.lang3.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class LegacyEventDeserializer {

	public static void deserializeTestEvents(byte[] contentBytes, SerializationConsumer<BatchedStoredTestEvent> action)
			throws Exception
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] teBytes = readNextData(dis);
				BatchedStoredTestEvent tempTe = deserializeTestEvent(teBytes);

				action.accept(tempTe);
			}
		}
	}
	
	
	public static void deserializeTestEventsMetadata(byte[] contentBytes, SerializationConsumer<BatchedStoredTestEventMetadata> action)
			throws Exception
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] teBytes = readNextData(dis);
				BatchedStoredTestEventMetadata tempTe = deserializeTestEventMetadata(teBytes);
				action.accept(tempTe);
			}
		}
	}

	private static byte[] readNextData(DataInputStream source) throws IOException
	{
		int size = source.readInt();
		byte[] result = new byte[size];
		source.read(result);
		return result;
	}

	private static BatchedStoredTestEvent deserializeTestEvent(byte[] bytes)
	{
		return (BatchedStoredTestEvent) org.apache.commons.lang3.SerializationUtils.deserialize(bytes);
	}

	private static BatchedStoredTestEventMetadata deserializeTestEventMetadata(byte[] bytes)
	{
		return (BatchedStoredTestEventMetadata) SerializationUtils.deserialize(bytes);
	}
	
}
