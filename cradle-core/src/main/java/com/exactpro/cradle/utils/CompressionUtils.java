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
