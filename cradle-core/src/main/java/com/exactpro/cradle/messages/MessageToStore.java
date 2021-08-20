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

package com.exactpro.cradle.messages;

import java.time.Instant;
import java.util.Arrays;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.books.BookId;
import com.exactpro.cradle.utils.CompressionUtils;

/**
 * Object to hold information about one message prepared to be stored in Cradle
 */
public class MessageToStore
{
	private BookId bookId;
	private String sessionAlias;
	private Direction direction;
	private Instant timestamp;
	private long sequence;
	private MessageMetadata metadata = null;
	private byte[] content;
	
	public MessageToStore()
	{
		sequence = -1;
	}
	
	public MessageToStore(MessageToStore copyFrom)
	{
		this.bookId = copyFrom.getBookId();
		this.sessionAlias = copyFrom.getSessionAlias();
		this.direction = copyFrom.getDirection();
		this.sequence = copyFrom.getSequence();
		this.timestamp = copyFrom.getTimestamp();
		this.metadata = copyFrom.getMetadata() != null ? new MessageMetadata(copyFrom.getMetadata()) : null;
		this.content = copyFrom.getContent();
	}
	
	
	/**
	 * @return book the message is related to
	 */
	public BookId getBookId()
	{
		return bookId;
	}
	
	public void setBook(BookId book)
	{
		this.bookId = book;
	}
	
	
	/**
	 * @return alias of session the message is related to
	 */
	public String getSessionAlias()
	{
		return sessionAlias;
	}
	
	public void setSessionAlias(String sessionAlias)
	{
		this.sessionAlias = sessionAlias;
	}
	
	
	/**
	 * @return direction in which the message went through the session
	 */
	public Direction getDirection()
	{
		return direction;
	}
	
	public void setDirection(Direction direction)
	{
		this.direction = direction;
	}
	
	
	/**
	 * @return timestamp of message creation
	 */
	public Instant getTimestamp()
	{
		return timestamp;
	}
	
	public void setTimestamp(Instant timestamp)
	{
		this.timestamp = timestamp;
	}
	
	
	/**
	 * @return sequence number the message has for its session, direction and timestamp
	 */
	public long getSequence()
	{
		return sequence;
	}
	
	public void setSequence(long sequence)
	{
		this.sequence = sequence;
	}
	
	
	public MessageMetadata getMetadata()
	{
		return metadata;
	}
	
	public void setMetadata(MessageMetadata metadata)
	{
		this.metadata = metadata;
	}
	
	public void addMetadata(String key, String value)
	{
		if (metadata == null)
			metadata = new MessageMetadata();
		metadata.add(key, value);
	}
	
	
	/**
	 * @return message content
	 */
	public byte[] getContent()
	{
		return content;
	}
	
	public void setContent(byte[] message)
	{
		this.content = message;
	}
	
	
	@Override
	public String toString()
	{
		return new StringBuilder()
				.append("MessageToStore{").append(CompressionUtils.EOL)
				.append("bookId=").append(bookId).append(",").append(CompressionUtils.EOL)
				.append("sessionAlias=").append(sessionAlias).append(",").append(CompressionUtils.EOL)
				.append("direction=").append(direction).append(",").append(CompressionUtils.EOL)
				.append("sequence=").append(sequence).append(",").append(CompressionUtils.EOL)
				.append("timestamp=").append(timestamp).append(",").append(CompressionUtils.EOL)
				.append("metadata=").append(metadata).append(",").append(CompressionUtils.EOL)
				.append("content=").append(Arrays.toString(content)).append(CompressionUtils.EOL)
				.append("}").toString();
	}
}
