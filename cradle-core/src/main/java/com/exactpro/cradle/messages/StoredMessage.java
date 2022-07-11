/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import java.util.Arrays;
import java.util.Objects;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.utils.CompressionUtils;

/**
 * Holds information about one message stored in Cradle.
 */
public class StoredMessage implements Serializable {

	private static final long serialVersionUID = 200983136307497672L;
	public static final String METADATA_KEY_PROTOCOL = "com.exactpro.th2.cradle.grpc.protocol";
	
	private final StoredMessageId id;
	private final Instant timestamp;
	private final StoredMessageMetadata metadata;
	private final byte[] content;

	public StoredMessage(MessageToStore message, StoredMessageId id) {
		this.id = id;
		this.timestamp = message.getTimestamp();
		this.metadata = message.getMetadata() != null ? new StoredMessageMetadata(message.getMetadata()) : null;
		this.content = message.getContent();
	}
	
	public StoredMessage(StoredMessage copyFrom, StoredMessageId id) {
		this.id = id;
		this.timestamp = copyFrom.getTimestamp();
		this.metadata = copyFrom.getMetadata() != null ? new StoredMessageMetadata(copyFrom.getMetadata()) : null;
		this.content = copyFrom.getContent();
	}
	
	public StoredMessage(StoredMessage copyFrom) {
		this(copyFrom, copyFrom.getId());
	}

	protected StoredMessage(StoredMessageId id, Instant timestamp, StoredMessageMetadata metadata, byte[] content) {
		this.id = id;
		this.timestamp = timestamp;
		this.metadata = metadata;
		this.content = content;
	}

	/**
	 * @return unique message ID as stored in Cradle.
	 * Result of this method should be used for referencing stored messages to obtain them from Cradle
	 */
	public StoredMessageId getId()
	{
		return id;
	}
	
	/**
	 * @return name of stream the message is related to
	 */
	public String getStreamName()
	{
		return id.getStreamName();
	}
	
	/**
	 * @return direction in which the message went through the stream
	 */
	public Direction getDirection()
	{
		return id.getDirection();
	}
	
	/**
	 * @return index the message has for its stream and direction
	 */
	public long getIndex()
	{
		return id.getIndex();
	}
	
	/**
	 * @return timestamp of message creation
	 */
	public Instant getTimestamp()
	{
		return timestamp;
	}
	
	/**
	 * @return metadata attached to message
	 */
	public StoredMessageMetadata getMetadata()
	{
		return metadata;
	}
	
	/**
	 * @return message content
	 */
	public byte[] getContent()
	{
		return content;
	}

	/**
	 * @return grpc protocol
	 */
	public String getProtocol() {
		return metadata == null ? null : metadata.get(METADATA_KEY_PROTOCOL);
	}


	@Override
	public int hashCode() {
		return Objects.hash(content, id, metadata, timestamp);
	}


	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (! (o instanceof StoredMessage))
			return false;

		StoredMessage other = (StoredMessage) o;
		return 	Objects.equals(id, other.id) &&
				Objects.equals(metadata, other.metadata) &&
				Objects.equals(timestamp, other.timestamp) &&
				Arrays.equals(content, other.content);
	}


	@Override
	public String toString() {

		return new StringBuilder()
				.append("StoredMessage{").append(CompressionUtils.EOL)
				.append("ID=").append(id).append(",").append(CompressionUtils.EOL)
				.append("streamName=").append(getStreamName()).append(",").append(CompressionUtils.EOL)
				.append("direction=").append(getDirection()).append(",").append(CompressionUtils.EOL)
				.append("index=").append(getIndex()).append(",").append(CompressionUtils.EOL)
				.append("timestamp=").append(getTimestamp()).append(",").append(CompressionUtils.EOL)
				.append("metadata=").append(getMetadata()).append(",").append(CompressionUtils.EOL)
				.append("content=").append(Arrays.toString(getContent())).append(CompressionUtils.EOL)
				.append("}").toString();
	}
}
