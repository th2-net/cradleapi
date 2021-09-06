/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao;

import java.util.Random;

public class TestUtils
{
	private static final Random random = new Random();

	public static byte[] createContent(int size)
	{
		StringBuilder sb = new StringBuilder(size);
		for (int i = 0; i < size; i++)
			sb.append((char)(random.nextInt(10)+48));
		return sb.toString().getBytes();
	}

}
