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

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.utils.CradleIdException;

/**
 * Holds ID of a message stored in Cradle.
 * All messages have sequence number, scoped by direction and session the message is related to.
 * Message sequence in conjunction with session alias, direction of the message and its timestamp form the message ID
 */
public class StoredMessageId implements Serializable
{
	private static final long serialVersionUID = -6014720618704186254L;
	public static final String ID_PARTS_DELIMITER = ":";
	
	private final BookId bookId;
	private final String sessionAlias;
	private final Direction direction;
	private final Instant timestamp;
	private final long sequence;
	
	public StoredMessageId(BookId bookId, String sessionAlias, Direction direction, Instant timestamp, long sequence)
	{
		this.bookId = bookId;
		this.sessionAlias = sessionAlias;
		this.direction = direction;
		this.timestamp = timestamp;
		this.sequence = sequence;
	}
	
	
	public static StoredMessageId fromString(String id) throws CradleIdException
	{
		String[] parts = StoredMessageIdUtils.splitParts(id);
		
		long seq = StoredMessageIdUtils.getSequence(parts);
		Instant timestamp = StoredMessageIdUtils.getTimestamp(parts);
		Direction direction = StoredMessageIdUtils.getDirection(parts);
		String session = StoredMessageIdUtils.getSessionAlias(parts);
		BookId book = StoredMessageIdUtils.getBookId(parts);
		return new StoredMessageId(book, session, direction, timestamp, seq);
	}
	
	
	public BookId getBookId()
	{
		return bookId;
	}
	
	public String getSessionAlias()
	{
		return sessionAlias;
	}
	
	public Direction getDirection()
	{
		return direction;
	}
	
	public Instant getTimestamp()
	{
		return timestamp;
	}
	
	public long getSequence()
	{
		return sequence;
	}
	
	
	@Override
	public String toString()
	{
		return bookId+ID_PARTS_DELIMITER
				+sessionAlias+ID_PARTS_DELIMITER
				+direction.getLabel()+ID_PARTS_DELIMITER
				+StoredMessageIdUtils.timestampToString(timestamp)+ID_PARTS_DELIMITER
				+sequence;
	}


	@Override
	public int hashCode()
	{
		return Objects.hash(bookId, sessionAlias, direction, timestamp, sequence);
	}


	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StoredMessageId other = (StoredMessageId) obj;
		return Objects.equals(bookId, other.bookId) && Objects.equals(sessionAlias, other.sessionAlias)
				&& direction == other.direction && Objects.equals(timestamp, other.timestamp)
				&& sequence == other.sequence;
	}
}