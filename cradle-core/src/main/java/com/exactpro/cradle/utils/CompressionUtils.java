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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressionUtils {
	public static String EOL = "\r\n";
	public static int BUFFER_SIZE = 4096;
	private static final boolean USE_LZ4 = Boolean.getBoolean("USE_LZ4");
	private static final LZ4Compressor COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
	private static final LZ4FastDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().fastDecompressor();

	static {
		if (USE_LZ4) {
			System.out.println("Using LZ4 compression");
		} else {
			System.out.println("Using ZIP compression");
		}
	}

	public static byte[] compressData(byte[] data) throws IOException {
		return USE_LZ4 ? compressLz4(data) : compressZip(data);
	}

	public static byte[] compressLz4(byte[] data) {
		var length = data.length;
		var maxLength = COMPRESSOR.maxCompressedLength(length);
		var compressed = new byte[Integer.BYTES + maxLength];
		var newLength = COMPRESSOR.compress(data, 0, length, compressed, Integer.BYTES, maxLength);
		var buffer = ByteBuffer.wrap(compressed);
		buffer.putInt(0, length);
		return Arrays.copyOf(compressed, Integer.BYTES + newLength);
	}

	public static byte[] compressZip(byte[] data) throws IOException {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length)) {
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

	public static byte[] decompressData(byte[] data) throws IOException, DataFormatException {
		return USE_LZ4 ? decompressLz4(data) : decompressZip(data);
	}

	public static byte[] decompressLz4(byte[] data) {
		return DECOMPRESSOR.decompress(data, Integer.BYTES, ByteBuffer.wrap(data).getInt());
	}

	public static byte[] decompressZip(byte[] data) throws IOException, DataFormatException {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length)) {
			Inflater inflater = new Inflater();
			inflater.setInput(data);
			byte[] buffer = new byte[BUFFER_SIZE];
			while (!inflater.finished()) {
				int count = inflater.inflate(buffer);
				outputStream.write(buffer, 0, count);
			}
			inflater.end();
			return outputStream.toByteArray();
		}
	}
}
