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

package com.exactpro.cradle.testevents;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;

/**
 * Holds information about single (individual) test event stored in Cradle
 */
public class StoredTestEventSingle extends StoredTestEvent implements TestEventSingle
{
	private final Instant endTimestamp;
	private final boolean success;
	private final Set<StoredMessageId> messages;
	private final ByteBuffer contentBuffer;

	private final int arrayOffset;
	private final int remaining;
	private final AtomicReference<byte[]> content = new AtomicReference<>();

	public StoredTestEventSingle(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
								 Instant endTimestamp, boolean success, ByteBuffer content,
								 Set<StoredMessageId> eventMessages, PageId pageId, String error, Instant recDate) {
		super(id, name, type, parentId, pageId, error, recDate);
		
		this.endTimestamp = endTimestamp;
		this.success = success;

		this.contentBuffer = content;
		if (this.contentBuffer == null) {
			arrayOffset = -1;
			remaining = -1;
		} else {
			arrayOffset = this.contentBuffer.arrayOffset();
			remaining = this.contentBuffer.remaining();
		}

		this.messages = eventMessages != null && !eventMessages.isEmpty() ? Set.copyOf(eventMessages) : null;
	}

	/**
	 * @deprecated this api is deprecated by read performance reason.<br>
	 * 				Migrate to {@link StoredTestEventSingle#StoredTestEventSingle(StoredTestEventId, String, String, StoredTestEventId, Instant, boolean, ByteBuffer, Set, PageId, String, Instant)}
	 */
	@Deprecated(since = "5.6.0")
	public StoredTestEventSingle(StoredTestEventId id, String name, String type, StoredTestEventId parentId,
								 Instant endTimestamp, boolean success, byte[] content,
								 Set<StoredMessageId> eventMessages, PageId pageId, String error, Instant recDate) {
		this(id, name, type, parentId, endTimestamp, success, ByteBuffer.wrap(content),
				eventMessages, pageId, error, recDate);
	}

	public StoredTestEventSingle(TestEventSingle event, PageId pageId) {
		this(event.getId(), event.getName(), event.getType(), event.getParentId(),
				event.getEndTimestamp(), event.isSuccess(), event.getContentBuffer(),
				event.getMessages(), pageId, null, null);
	}
	
	
	@Override
	public Instant getEndTimestamp()
	{
		return endTimestamp;
	}
	
	@Override
	public boolean isSuccess()
	{
		return success;
	}
	
	@Override
	public Set<StoredMessageId> getMessages()
	{
		return messages;
	}
	
	@Override
	@Deprecated
	public byte[] getContent() {
		if (contentBuffer == null) { return null; }
		return content.accumulateAndGet(null, (curr, x) -> {
			if (curr == null) {
				byte[] result = new byte[remaining];
				contentBuffer.mark();
				contentBuffer.get(result, arrayOffset, remaining);
				contentBuffer.reset();
				return result;
			}
			return curr;
		});
	}

	@Override
	public ByteBuffer getContentBuffer() {
		return contentBuffer;
	}

	@Override
	public Instant getLastStartTimestamp() {
		return getStartTimestamp();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof StoredTestEventSingle)) return false;
		StoredTestEventSingle single = (StoredTestEventSingle) o;
		return isSuccess() == single.isSuccess()
				&& Objects.equals(getEndTimestamp(), single.getEndTimestamp())
				&& Objects.equals(getMessages(), single.getMessages())
				&& Arrays.equals(getContent(), single.getContent());
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(getEndTimestamp(), isSuccess(), getMessages());
		result = 31 * result + Arrays.hashCode(getContent());
		return result;
	}

	@Override
	public String toString() {
		String contentAsText = contentBuffer == null
				? null : StandardCharsets.UTF_8.decode(contentBuffer.asReadOnlyBuffer()).toString();
		return "StoredTestEventSingle{" +
				"endTimestamp=" + endTimestamp +
				", success=" + success +
				", messages=" + messages +
				", content=" + contentAsText +
				'}';
	}
}
