package com.exactpro.cradle.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;

import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatchId;
import com.exactpro.cradle.testevents.StoredTestEventId;

public class TestEventUtils
{
	public static byte[] serializeTestEvents(StoredTestEvent[] testEvents) throws IOException
	{
		byte[] batchContent;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(out))
		{
			for (StoredTestEvent te : testEvents)
			{
				if (te == null)  //For case of not full batch
					break;
				
				byte[] serializedTe = SerializationUtils.serialize(te);
				dos.writeInt(serializedTe.length);
				dos.write(serializedTe);
			}
			dos.flush();
			batchContent = out.toByteArray();
		}
		return batchContent;
	}
	
	public static StoredTestEvent deserializeOneTestEvent(byte[] contentBytes, StoredTestEventId id, StoredTestEventId parentId) throws IOException
	{
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			int index = -1;
			while (dis.available() != 0)
			{
				index++;
				byte[] teBytes = readNextTestEventBytes(dis);
				if (id.getIndex() != index)
					continue;
				
				return deserializeTestEvent(teBytes, id, parentId);
			}
		}
		
		return null;
	}

	public static Collection<StoredTestEvent> deserializeTestEvents(byte[] contentBytes, StoredTestEventBatchId batchId, StoredTestEventId parentId) throws IOException
	{
		List<StoredTestEvent> result = new ArrayList<>();
		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(contentBytes)))
		{
			int index = -1;
			while (dis.available() != 0)
			{
				index++;
				byte[] teBytes = readNextTestEventBytes(dis);
				StoredTestEvent tempTe = deserializeTestEvent(teBytes, new StoredTestEventId(batchId, index), parentId);
				result.add(tempTe);
			}
		}
		return result;
	}
	
	
	private static byte[] readNextTestEventBytes(DataInputStream dis) throws IOException
	{
		int teSize = dis.readInt();
		byte[] result = new byte[teSize];
		dis.read(result);
		return result;
	}
	
	private static StoredTestEvent deserializeTestEvent(byte[] bytes, StoredTestEventId id, StoredTestEventId parentId)
	{
		StoredTestEvent result = (StoredTestEvent)SerializationUtils.deserialize(bytes);
		result.setId(id);
		result.setParentId(parentId);
		return result;
	}
}
