/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

public class CradleSerializationUtils
{
	public static void writeString(String s, DataOutputStream dos) throws IOException
	{
		byte[] bytes = s.getBytes();
		dos.writeShort(bytes.length);
		dos.write(bytes);
	}

	public static void writeString(String s, ByteBuffer buffer) {
		byte[] bytes = s.getBytes();
		buffer.putShort((short) bytes.length);
		buffer.put(bytes);
	}
	
	public static String readString(DataInputStream dis) throws IOException
	{
		int length = dis.readShort();
		byte[] bytes = new byte[length];
		dis.readFully(bytes);
		return new String(bytes);
	}
	
	
	public static void writeInstant(Instant i, DataOutputStream dos) throws IOException
	{
		dos.writeLong(i.getEpochSecond());
		dos.writeInt(i.getNano());
	}

	public static void writeInstant(Instant i, ByteBuffer buffer) {
		buffer.putLong(i.getEpochSecond());
		buffer.putInt(i.getNano());
	}
	
	public static Instant readInstant(DataInputStream dis) throws IOException
	{
		return Instant.ofEpochSecond(dis.readLong(), dis.readInt());
	}
}
