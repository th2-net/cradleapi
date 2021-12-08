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

package com.exactpro.cradle.testevents;

import java.time.Instant;

public class BatchedStoredTestEventMetadataBuilder {

	private StoredTestEventId id;
	private String name,
			type;
	private StoredTestEventId parentId;
	private Instant startTimestamp,
			endTimestamp;
	private boolean success;

	private StoredTestEventBatchMetadata batch;

	public BatchedStoredTestEventMetadataBuilder setId(StoredTestEventId id) {
		this.id = id;
		return this;
	}

	public BatchedStoredTestEventMetadataBuilder setName(String name) {
		this.name = name;
		return this;
	}

	public BatchedStoredTestEventMetadataBuilder setType(String type) {
		this.type = type;
		return this;
	}

	public BatchedStoredTestEventMetadataBuilder setParentId(StoredTestEventId parentId) {
		this.parentId = parentId;
		return this;
	}

	public BatchedStoredTestEventMetadataBuilder setStartTimestamp(Instant startTimestamp) {
		this.startTimestamp = startTimestamp;
		return this;
	}

	public BatchedStoredTestEventMetadataBuilder setEndTimestamp(Instant endTimestamp) {
		this.endTimestamp = endTimestamp;
		return this;
	}

	public BatchedStoredTestEventMetadataBuilder setSuccess(boolean success) {
		this.success = success;
		return this;
	}

	public BatchedStoredTestEventMetadataBuilder setBatch(StoredTestEventBatchMetadata batch) {
		this.batch = batch;
		return this;
	}

	public BatchedStoredTestEventMetadata build() {
		return new BatchedStoredTestEventMetadata(this.id, this.name, this.type, this.parentId, this.startTimestamp,
				this.endTimestamp, this.success, this.batch);
	}
}
