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

package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class MessageEntityUtils
{
	private static final Logger logger = LoggerFactory.getLogger(MessageEntityUtils.class);

	public static Collection<MessageBatchEntity> toEntities(StoredMessageBatch batch, PageId pageId, int maxUncompressedSize,
			int contentChunkSize) throws IOException
	{
		byte[] batchContent = MessageUtils.serializeMessages(batch.getMessages());
		boolean compressed = batchContent.length > maxUncompressedSize;
		if (compressed)
		{
			StoredMessageId batchId = batch.getId();
			try
			{
				logger.trace("Compressing content of message batch {}", batchId);
				batchContent = CompressionUtils.compressData(batchContent);
			}
			catch (IOException e)
			{
				throw new IOException(String.format("Could not compress message batch contents (ID: '%s') to save in Cradle",
						batchId.toString()), e);
			}
		}
		return toEntities(batch, pageId, batchContent, compressed, contentChunkSize);
	}

	public static Collection<MessageBatchEntity> toEntities(StoredMessageBatch batch, PageId pageId, byte[] content,
			boolean compressed, int contentChunkSize)
	{
		Collection<MessageBatchEntity> result = new ArrayList<>();
		if (content == null)
			return result;

		logger.debug("Creating chunks from messages batch '{}'", batch.getId());
		int length = content.length;
		for (int i = 0, chunk = 0; i < length; i+=contentChunkSize, chunk++)
		{
			int endOfRange = Math.min(length, i + contentChunkSize);
			byte[] entityContent = Arrays.copyOfRange(content, i, endOfRange);
			MessageBatchEntity entity =
					new MessageBatchEntity(batch, pageId, entityContent, compressed, chunk, endOfRange == length);
			result.add(entity);
		}

		return result;
	}


}
