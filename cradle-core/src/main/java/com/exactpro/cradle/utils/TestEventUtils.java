package com.exactpro.cradle.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.zip.DataFormatException;

import org.apache.commons.lang3.SerializationUtils;

import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;

public class TestEventUtils
{
	/**
	 * Checks that test event has all necessary fields set
	 * @param event to validate
	 * @throws CradleStorageException if validation failed
	 */
	public static void validateTestEvent(StoredTestEvent event) throws CradleStorageException
	{
		if (event.getId() == null)
			throw new CradleStorageException("Test event ID cannot be null");
		if (event.getId().equals(event.getParentId()))
			throw new CradleStorageException("Test event cannot reference itself");
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
			{
				byte[] serializedTe = SerializationUtils.serialize(te);
				dos.writeInt(serializedTe.length);
				dos.write(serializedTe);
			}
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
				byte[] teBytes = readNextTestEventBytes(dis);
				BatchedStoredTestEvent tempTe = deserializeTestEvent(teBytes);
				if (tempTe.getParentId() == null)  //Workaround to fix events stored before commit f71b224e6f4dc0c8c99512de6a8f2034a1c3badc. TODO: remove it in future
				{
					TestEventToStore te = TestEventToStore.builder()
							.id(tempTe.getId())
							.name(tempTe.getName())
							.type(tempTe.getType())
							.parentId(batch.getParentId())
							.startTimestamp(tempTe.getStartTimestamp())
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
	
	
	private static byte[] readNextTestEventBytes(DataInputStream dis) throws IOException
	{
		int teSize = dis.readInt();
		byte[] result = new byte[teSize];
		dis.read(result);
		return result;
	}
	
	private static BatchedStoredTestEvent deserializeTestEvent(byte[] bytes)
	{
		return (BatchedStoredTestEvent)SerializationUtils.deserialize(bytes);
	}
}
