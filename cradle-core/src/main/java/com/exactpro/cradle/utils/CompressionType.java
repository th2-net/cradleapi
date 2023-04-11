/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

public enum CompressionType {
    ZLIB {
        public static final int BUFFER_SIZE = 4096;

        @Override
        public byte[] compress(byte[] data) throws CompressException {
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
            } catch (IOException e) {
                throw new CompressException("Data can't be compressed", e);
            }
        }

        @Override
        public byte[] decompress(byte[] data) throws CompressException {
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
            } catch (IOException | DataFormatException e) {
                throw new CompressException("Data can't be decompressed", e);
            }
        }
    },
    LZ4 {
        private final LZ4Compressor COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
        private final LZ4FastDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().fastDecompressor();

        @Override
        public byte[] compress(byte[] data) {
            var length = data.length;
            var maxLength = COMPRESSOR.maxCompressedLength(length);
            var compressed = new byte[Integer.BYTES + maxLength];
            var newLength = COMPRESSOR.compress(data, 0, length, compressed, Integer.BYTES, maxLength);
            var buffer = ByteBuffer.wrap(compressed);
            buffer.putInt(0, length);
            return Arrays.copyOf(compressed, Integer.BYTES + newLength);
        }

        @Override
        public byte[] decompress(byte[] data) {
            return DECOMPRESSOR.decompress(data, Integer.BYTES, ByteBuffer.wrap(data).getInt());
        }
    };

    public abstract byte[] compress(byte[] data) throws CompressException;

    public abstract byte[] decompress(byte[] data) throws CompressException;

    // https://www.rfc-editor.org/rfc/rfc1950
    // 78 01 - No Compression/low
    // 78 9C - Default Compression - used in our case
    // 78 DA - Best Compression
    public static final byte ZLIB_CMF = 0x78;
    public static final byte ZLIB_FLG = (byte) 0x9C;

    public static byte[] decompressData(byte[] data) throws CompressException {
        if (data.length < 2) {
            throw new CompressException("Data too short. Compression format is undetected");
        }
        if (data[0] == ZLIB_CMF && data[1] == ZLIB_FLG) {
            return ZLIB.decompress(data);
        } else {
            return LZ4.decompress(data);
        }
    }
}
