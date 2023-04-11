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

import org.testng.annotations.Test;

import java.util.Random;

import static com.exactpro.cradle.utils.CompressionType.LZ4;
import static com.exactpro.cradle.utils.CompressionType.ZLIB;
import static org.testng.Assert.assertEquals;

public class CompressionTypeTest {
    @Test
    public void compressDecompressZLIB() throws CompressException {
        byte[] bytes = new byte[1024 * 16];
        new Random().nextBytes(bytes);

        byte[] compressed = ZLIB.compress(bytes);
        assertEquals(bytes, ZLIB.decompress(compressed), "Decompress via " + ZLIB);
        assertEquals(bytes, CompressionType.decompressData(compressed), "Decompress via the decompressData method");
    }

    @Test
    public void compressDecompressZL4() throws CompressException {
        byte[] bytes = new byte[1024 * 16];
        new Random().nextBytes(bytes);

        byte[] compressed = LZ4.compress(bytes);
        assertEquals(bytes, LZ4.decompress(compressed), "Decompress via " + LZ4);
        assertEquals(bytes, CompressionType.decompressData(compressed), "Decompress via the decompressData method");
    }
}
