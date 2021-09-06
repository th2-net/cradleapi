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
import com.exactpro.cradle.cassandra.dao.EntityUtils;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.messages.MessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

public class MessageEntityUtils
{
	private static final Logger logger = LoggerFactory.getLogger(MessageEntityUtils.class);

	public static Collection<MessageBatchEntity> toEntities(MessageBatch batch, PageId pageId, int maxUncompressedSize,
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

	public static Collection<MessageBatchEntity> toEntities(MessageBatch batch, PageId pageId, byte[] content,
			boolean compressed, int contentChunkSize)
	{
		Collection<MessageBatchEntity> result = new ArrayList<>();
		if (content == null)
			return result;

		int length = content.length;
		logger.debug("Creating chunks from messages batch '{}' of {} bytes", batch.getId(), length);
		for (int i = 0; i < length; i+=contentChunkSize)
		{
			int endOfRange = Math.min(length, i + contentChunkSize);
			byte[] entityContent = Arrays.copyOfRange(content, i, endOfRange);
			boolean isLastChunk = endOfRange == length;
			if (logger.isDebugEnabled())
				logger.debug("Creating chunk #{}{} of {} bytes from messages batch '{}'",
						result.size()+1,
						isLastChunk ? " (last one)" : "",
						entityContent.length,
						batch.getId());
			MessageBatchEntity entity =
					new MessageBatchEntity(batch, pageId, entityContent, compressed, result.size(), isLastChunk);
			result.add(entity);
		}

		return result;
	}

	public static StoredMessageBatch toStoredMessageBatch(Collection<MessageBatchEntity> entities, PageId pageId)
			throws DataFormatException, IOException
	{
		if (entities == null || entities.isEmpty())
			return null;
		logger.debug("Creating message batch from {} chunk(s)", entities.size());
		MessageBatchEntity entity = entities.iterator().next();
		//TODO Implement error message batch 
		//String error = EntityUtils.validateEntities(entities);
		StoredMessageId batchId = entity.createBatchId(pageId.getBookId());
		byte[] content = getContent(entities, batchId);
		List<StoredMessage> storedMessages = MessageUtils.deserializeMessages(content);
		return new StoredMessageBatch(storedMessages);
	}

	private static byte[] getContent(Collection<MessageBatchEntity> entities, StoredMessageId messageBatchId)
			throws DataFormatException, IOException
	{
		byte[] content = EntityUtils.uniteContents(entities);
		if (content == null)
			return null;
		MessageBatchEntity entity = entities.iterator().next();
		if (entity.isCompressed())
		{
			logger.trace("Decompressing content of message batch '{}'", messageBatchId);
			return CompressionUtils.decompressData(content);
		}
		return content;
	}
}
