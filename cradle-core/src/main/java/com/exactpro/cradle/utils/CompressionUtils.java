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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressionUtils
{
	public static String EOL = "\r\n";
	public static int BUFFER_SIZE = 4096;
	
	public static byte[] compressData(byte[] data) throws IOException
	{
		try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length))
		{
			Deflater deflater = new Deflater();
			deflater.setInput(data);
			deflater.finish();
			byte[] buffer = new byte[BUFFER_SIZE];
			while (!deflater.finished()) {
				int count = deflater.deflate(buffer);
				outputStream.write(buffer, 0, count);
			}
			deflater.end();
			return outputStream.toByteArray();
		}
	}

	public static byte[] decompressData(byte[] data) throws IOException, DataFormatException
	{
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length))
		{
			Inflater inflater = new Inflater();
			inflater.setInput(data);
			byte[] buffer = new byte[BUFFER_SIZE];
			while (!inflater.finished())
			{
				int count = inflater.inflate(buffer);
				outputStream.write(buffer, 0, count);
			}
			inflater.end();
			return outputStream.toByteArray();
		}
	}
}
