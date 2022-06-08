/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.serialization;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static com.exactpro.cradle.serialization.MessagesSizeCalculator.lenStr;
import static com.exactpro.cradle.serialization.Serialization.INVALID_MAGIC_NUMBER_FORMAT;

public class SerializationUtils {
	
	public static final int USHORT_MAX_VALUE = 65535;

	static void printBody(byte[] body, ByteBuffer buffer) {
		if (body == null) {
			buffer.putInt(-1);
		} else {
			buffer.putInt(body.length);
			buffer.put(body);	
		}
	}

	static void printShortString(String value, ByteBuffer buffer, String paramName) throws SerializationException {
		if (value == null) {
			value = "";
		}
		if (value.length() > USHORT_MAX_VALUE) {
			throw new SerializationException(String.format("%s is too big. Expected length [0-%s]", paramName, USHORT_MAX_VALUE));
		}
		buffer.putShort((short) lenStr(value));
		buffer.put(value.getBytes(StandardCharsets.UTF_8));
	}

	static void printString(String value, ByteBuffer buffer) {
		if (value == null) {
			buffer.putInt(-1);
		} else {
			buffer.putInt(lenStr(value));
			buffer.put(value.getBytes(StandardCharsets.UTF_8));
		}
	}

	static void printInstant(Instant instant, ByteBuffer buffer) {
		if (instant != null) {
			buffer.putLong(instant.getEpochSecond());
			buffer.putInt(instant.getNano());	
		} else {
			buffer.putLong(-1);
			buffer.putInt(-1);
		}
	}

	static byte[] readBody(ByteBuffer buffer) {
		int bodyLen = buffer.getInt();
		if (bodyLen < 0) {
			return null;
		}
		byte[] body = new byte[bodyLen];
		buffer.get(body);
		return body;
	}

	static String readString(ByteBuffer buffer) throws SerializationException {
		return readString(buffer, buffer.getInt());
	}

	static String readShortString(ByteBuffer buffer) throws SerializationException {
		return readString(buffer, Short.toUnsignedInt(buffer.getShort()));
	}

	private static String readString(ByteBuffer buffer, int len) throws SerializationException {
		if (len < 0)
			return null;

		if (buffer.remaining() < len) {
			throw new SerializationException(String.format("String to read (%d bytes) is longer than remaining buffer (%d)",
					len, buffer.remaining()));
		}
		int currPos = buffer.position();
		String str = new String(buffer.array(), currPos, len, StandardCharsets.UTF_8);
		buffer.position(currPos + len);
		return str;
	}

	static Instant readInstant(ByteBuffer buffer) {
		long seconds = buffer.getLong();
		int nanos = buffer.getInt();
		if (seconds == -1 && nanos == -1) {
			return null;
		} else {
			return Instant.ofEpochSecond(seconds, nanos);	
		}
		
	}

	static void printSingleBoolean(boolean instant, ByteBuffer buffer) {
		buffer.put((byte) (instant ? 1 : 0));
	}

	static boolean readSingleBoolean(ByteBuffer buffer) {
		return buffer.get() == 1;
	}
	
	static SerializationException incorrectMagicNumber(String name, int expected, int actual) {
		return new SerializationException(String.format(INVALID_MAGIC_NUMBER_FORMAT, name,
				Integer.toHexString(expected), Integer.toHexString(actual)));
	}

	static SerializationException incorrectMagicNumber(String name, short expected, short actual) {
		return new SerializationException(String.format(INVALID_MAGIC_NUMBER_FORMAT, name,
				Integer.toHexString(expected & 0xffff), Integer.toHexString(actual & 0xffff)));
	}
	
}
