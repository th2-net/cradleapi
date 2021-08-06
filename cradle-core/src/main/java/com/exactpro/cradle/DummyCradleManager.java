/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle;

/**
 * Dummy implementation of CradleManager that does nothing and serves as a stub
 */
public class DummyCradleManager extends CradleManager
{
	public DummyCradleManager(boolean prepareStorage)
	{
		super(prepareStorage);
	}

	@Override
	protected CradleStorage createStorage(String book, boolean prepareStorage)
	{
		return new DummyCradleStorage(book);
	}
}
