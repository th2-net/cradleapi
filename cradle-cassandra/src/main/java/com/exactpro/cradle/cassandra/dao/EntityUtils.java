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

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.exactpro.cradle.utils.CradleStorageException;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

public class EntityUtils
{
	public static byte[] uniteContents(Collection<? extends CradleEntity> entities)
	{
		int size = entities.stream()
				.mapToInt(e -> e.getContent() != null ? e.getContent().limit() : 0)
				.sum();

		if (size == 0)
			return null;

		ByteBuffer buffer = ByteBuffer.allocate(size);
		for (CradleEntity e : entities)
		{
			if (e.getContent() == null)
				continue;
			buffer.put(e.getContent());
		}
		return buffer.array();
	}

	public static String validateEntities(Collection<? extends CradleEntity> entities)
	{
		int chunkIndex = 0;
		for (CradleEntity entity : entities)
		{
			if (entity.getChunk() != chunkIndex)
				return "Chunk #"+chunkIndex+" is missing";

			if (chunkIndex == entities.size()-1 && !entity.isLastChunk())
				return "Last chunk is missing";

			chunkIndex++;
		}
		return null;
	}

	public static <T extends CradleEntity> List<T> toCompleteEntitiesCollection(MappedAsyncPagingIterable<T> resultSet)
			throws CradleStorageException
	{
		try
		{
			return DaoUtils.toList(resultSet);
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error while converting result set to collection", e);
		}
	}
}
