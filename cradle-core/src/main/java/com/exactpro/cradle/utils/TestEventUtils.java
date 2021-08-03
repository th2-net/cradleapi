/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.zip.DataFormatException;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.TestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;

public class TestEventUtils
{
	/**
	 * Checks that test event has all necessary fields set
	 * @param event to validate
	 * @param checkName indicates whether event name should be validated. For some events name is optional and thus shouldn't be checked
	 * @throws CradleStorageException if validation failed
	 */
	public static void validateTestEvent(TestEvent event, boolean checkName) throws CradleStorageException
	{
		if (event.getId() == null)
			throw new CradleStorageException("Test event ID cannot be null");
		if (event.getId().equals(event.getParentId()))
			throw new CradleStorageException("Test event cannot reference itself");
		if (checkName && StringUtils.isEmpty(event.getName()))
			throw new CradleStorageException("Test event must have a name");
		if (event.getStartTimestamp() == null)
			throw new CradleStorageException("Test event must have a start timestamp");
	}
	
	/**
	 * Serializes test events, skipping non-meaningful or calculatable fields
	 * @param testEvents to serialize
	 * @return array of bytes, containing serialized events
	 * @throws IOException if serialization failed
	 */
	public static byte[] serializeTestEvents(Collection<BatchedStoredTestEvent> testEvents) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(out))
		{
			for (BatchedStoredTestEvent te : testEvents)
				serialize(te, dos);
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}
	
	/**
	 * Deserializes all test events, adding them to given batch
	 * @param contentBytes to deserialize events from
	 * @param batch to add events to
	 * @throws IOException if deserialization failed
	 * @throws CradleStorageException if deserialized event doesn't match batch conditions
	 */
	public static void deserializeTestEvents(byte[] contentBytes, StoredTestEventBatch batch) 
			throws IOException, CradleStorageException
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			while (dis.available() != 0)
			{
				byte[] teBytes = readNextData(dis);
				BatchedStoredTestEvent tempTe = deserializeTestEvent(teBytes);
				if (tempTe.getParentId() == null)  //Workaround to fix events stored before commit f71b224e6f4dc0c8c99512de6a8f2034a1c3badc. TODO: remove it in future
				{
					TestEventSingleToStore te = TestEventSingleToStore.builder()
							.id(tempTe.getId())
							.name(tempTe.getName())
							.type(tempTe.getType())
							.parentId(batch.getParentId())
							.endTimestamp(tempTe.getEndTimestamp())
							.success(tempTe.isSuccess())
							.content(tempTe.getContent())
							.build();
					StoredTestEventBatch.addTestEvent(te, batch);
				}
				else
					StoredTestEventBatch.addTestEvent(tempTe, batch);
			}
		}
	}
	
	
	/**
	 * Decompresses given ByteBuffer and deserializes all test events, adding them to given batch
	 * @param content to deserialize events from
	 * @param compressed flag that indicates if content needs to be decompressed first
	 * @param batch to add events to
	 * @throws IOException if deserialization failed
	 * @throws CradleStorageException if deserialized event doesn't match batch conditions
	 */
	public static void bytesToTestEvents(ByteBuffer content, boolean compressed, StoredTestEventBatch batch) 
			throws IOException, CradleStorageException
	{
		byte[] contentBytes = getTestEventContentBytes(content, compressed, batch.getId());
		deserializeTestEvents(contentBytes, batch);
	}
	
	public static byte[] getTestEventContentBytes(ByteBuffer content, boolean compressed, StoredTestEventId eventId) throws IOException
	{
		byte[] contentBytes = content.array();
		if (!compressed)
			return contentBytes;
		
		try
		{
			return CompressionUtils.decompressData(contentBytes);
		}
		catch (IOException | DataFormatException e)
		{
			throw new IOException(String.format("Could not decompress content of test event (ID: '%s') from Cradle", eventId), e);
		}
	}
	
	
	private static void serialize(Serializable data, DataOutputStream target) throws IOException
	{
		byte[] serializedData = SerializationUtils.serialize(data);
		target.writeInt(serializedData.length);
		target.write(serializedData);
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
		return (BatchedStoredTestEvent)SerializationUtils.deserialize(bytes);
	}
}
