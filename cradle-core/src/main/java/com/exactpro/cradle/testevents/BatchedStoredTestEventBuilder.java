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

package com.exactpro.cradle.testevents;

import com.exactpro.cradle.PageId;

import java.nio.ByteBuffer;
import java.time.Instant;

public class BatchedStoredTestEventBuilder {

	private StoredTestEventId id;
	private String name,
			type;
	private StoredTestEventId parentId;
	private Instant endTimestamp;

	private boolean success;
	private ByteBuffer content;
	private PageId pageId;

	private StoredTestEventBatch batch;

	public BatchedStoredTestEventBuilder setId(StoredTestEventId id) {
		this.id = id;
		return this;
	}

	public BatchedStoredTestEventBuilder setName(String name) {
		this.name = name;
		return this;
	}

	public BatchedStoredTestEventBuilder setType(String type) {
		this.type = type;
		return this;
	}

	public BatchedStoredTestEventBuilder setParentId(StoredTestEventId parentId) {
		this.parentId = parentId;
		return this;
	}

	public BatchedStoredTestEventBuilder setEndTimestamp(Instant endTimestamp) {
		this.endTimestamp = endTimestamp;
		return this;
	}

	public BatchedStoredTestEventBuilder setSuccess(boolean success) {
		this.success = success;
		return this;
	}

	/**
	 * @deprecated this api is deprecated by read performance reason.<br>
	 * 				Migrate to {@link #setContent(ByteBuffer)}
	 */
	@Deprecated(since = "5.6.0")
	public BatchedStoredTestEventBuilder setContent(byte[] content) {
		this.content = ByteBuffer.wrap(content);
		return this;
	}

	public BatchedStoredTestEventBuilder setContent(ByteBuffer content) {
		this.content = content;
		return this;
	}

	public BatchedStoredTestEventBuilder setBatch(StoredTestEventBatch batch) {
		this.batch = batch;
		return this;
	}

	public BatchedStoredTestEventBuilder setPageId(PageId pageId) {
		this.pageId = pageId;
		return this;
	}

	public BatchedStoredTestEvent build() {
		return new BatchedStoredTestEvent(this.id, this.name, this.type, this.parentId,
				this.endTimestamp, this.success, this.content, this.batch, this.pageId);
	}
}
