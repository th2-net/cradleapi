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
