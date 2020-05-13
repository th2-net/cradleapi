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

import java.io.IOException;
import java.util.Random;
import java.util.zip.DataFormatException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CompressionUtilsTest
{
	@Test
	public void compressDecompress() throws IOException, DataFormatException
	{
		byte[] bytes = new byte[CompressionUtils.BUFFER_SIZE*3];
		new Random().nextBytes(bytes);
		byte[] compressed = CompressionUtils.compressData(bytes);
		Assert.assertEquals(CompressionUtils.decompressData(compressed), bytes, "Compressed/decompressed data");
	}
}
