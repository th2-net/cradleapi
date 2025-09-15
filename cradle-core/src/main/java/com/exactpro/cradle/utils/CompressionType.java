/*
 * Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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

import com.google.common.io.BaseEncoding;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public enum CompressionType {
    ZLIB {
        private static final int BUFFER_SIZE = 4096;
        // https://www.rfc-editor.org/rfc/rfc1950
        // 78 01 - No Compression/low
        // 78 9C - Default Compression - used in our case
        // 78 DA - Best Compression
        private static final byte CMF = 0x78;
        private static final byte FLG = (byte) 0x9C;
        private final byte[] MAGIC_BYTES = new byte[] { CMF, FLG };

        @Override
        public byte[] getMagicBytes() {
            return MAGIC_BYTES;
        }

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

        @Override
        public ByteBuffer decompress(ByteBuffer src, ByteBuffer dest) throws CompressException {
            Inflater inflater = new Inflater();
            inflater.setInput(src);

            try {
                while (!inflater.finished()) {
                    int written = inflater.inflate(dest);
                    if (written == 0) {
                        if (inflater.needsInput()) break;
                        if (inflater.needsDictionary()) {
                            throw new IllegalStateException("Preset dictionary required");
                        }
                    }
                }
                dest.flip();
                return dest;
            } catch (DataFormatException e) {
                throw new CompressException("Data can't be decompressed", e);
            } finally {
                inflater.end();
            }
        }
    },
    LZ4 {
        private final LZ4Compressor COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
        private final LZ4FastDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().fastDecompressor();
        // <null>LZ4 (ASCII)
        private final byte[] MAGIC_BYTES = ByteBuffer.allocate(4).putInt(0x004C5A34).array();
        private final int PREFIX_SIZE = MAGIC_BYTES.length + Integer.BYTES;

        @Override
        protected byte[] getMagicBytes() {
            return MAGIC_BYTES;
        }

        @Override
        public byte[] compress(byte[] data) {
            var length = data.length;
            var maxLength = COMPRESSOR.maxCompressedLength(length);
            var compressed = new byte[PREFIX_SIZE + maxLength];
            var newLength = COMPRESSOR.compress(data, 0, length, compressed,  PREFIX_SIZE, maxLength);
            var buffer = ByteBuffer.wrap(compressed);
            buffer.put(MAGIC_BYTES, 0, MAGIC_BYTES.length);
            buffer.putInt(MAGIC_BYTES.length, length);
            return Arrays.copyOf(compressed, PREFIX_SIZE + newLength);
        }

        @Override
        public byte[] decompress(byte[] data) {
            return DECOMPRESSOR.decompress(data, MAGIC_BYTES.length + Integer.BYTES, ByteBuffer.wrap(data).getInt(MAGIC_BYTES.length));
        }

        @Override
        public ByteBuffer decompress(ByteBuffer src, ByteBuffer dest) {
            DECOMPRESSOR.decompress(src, MAGIC_BYTES.length + Integer.BYTES, dest, dest.position(), dest.remaining());
            return dest;
        }
    };

    public abstract byte[] compress(byte[] data) throws CompressException;
    public abstract byte[] decompress(byte[] data) throws CompressException;
    public abstract ByteBuffer decompress(ByteBuffer src, ByteBuffer dest) throws CompressException;

    public boolean isDecompressable(byte[] data) {
        byte[] magicBytes = getMagicBytes();
        if (magicBytes.length > data.length) {
            return false;
        }
        for (int i = 0; i < magicBytes.length; i++) {
            if (magicBytes[i] != data[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean isDecompressable(ByteBuffer buffer) {
        byte[] magicBytes = getMagicBytes();
        if (magicBytes.length > buffer.remaining()) {
            return false;
        }
        for (int i = 0; i < magicBytes.length; i++) {
            if (magicBytes[i] != buffer.get(buffer.position() + i)) {
                return false;
            }
        }
        return true;
    }

    protected abstract byte[] getMagicBytes();

    public static byte[] decompressData(byte[] data) throws CompressException {
        if (data == null || data.length == 0) {
            throw new CompressException("Data is empty or null");
        }
        if (ZLIB.isDecompressable(data)) {
            return ZLIB.decompress(data);
        } else if (LZ4.isDecompressable(data)) {
            return LZ4.decompress(data);
        } else {
            BaseEncoding encoder = BaseEncoding.base16();
            throw new CompressException("Compression format is undetected. Decoded data: " + encoder.encode(data, 0, Math.min(10, data.length))
                    + ", supported formats: " + Arrays.stream(CompressionType.values())
                    .map(type -> type.name() + ":[" + encoder.encode(type.getMagicBytes()) + "]")
                    .collect(Collectors.joining(",")));
        }
    }

    public static ByteBuffer decompressData(ByteBuffer src, ByteBuffer dest) throws CompressException {
        if (src == null || src.remaining() == 0) {
            throw new CompressException("Data is empty or null");
        }
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType.isDecompressable(src)) {
                return compressionType.decompress(src, dest);
            }
        }
        BaseEncoding encoder = BaseEncoding.base16();
        throw new CompressException("Compression format is undetected. Decoded data: "
                + encoder.encode(src.array(), 0, Math.min(10, src.remaining()))
                + ", supported formats: " + Arrays.stream(CompressionType.values())
                .map(type -> type.name() + ":[" + encoder.encode(type.getMagicBytes()) + "]")
                .collect(Collectors.joining(",")));
    }
}
