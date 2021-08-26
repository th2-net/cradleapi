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

import java.io.Serializable;
import java.util.Collection;

/**
 * Holds information about one test event stored in batch of events ({@link TestEventBatch})
 */
public class BatchedStoredTestEvent extends StoredTestEventSingle implements Serializable
{
	private static final long serialVersionUID = -1350827714114261304L;
	
	private final transient TestEventBatch batch;
	
	public BatchedStoredTestEvent(TestEventSingle event, TestEventBatch batch)
	{
		super(event);
		
		this.batch = batch;
	}
	
	
	public StoredTestEventId getBatchId()
	{
		return batch.getId();
	}
	
	public boolean hasChildren()
	{
		return batch.hasChildren(getId());
	}
	
	public Collection<BatchedStoredTestEvent> getChildren()
	{
		return batch.getChildren(getId());
	}
}
