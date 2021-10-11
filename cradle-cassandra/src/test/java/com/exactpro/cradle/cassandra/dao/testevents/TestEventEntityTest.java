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

package com.exactpro.cradle.cassandra.dao.testevents;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

public class TestEventEntityTest
{
	private static final StoredTestEventId PARENT_ID = new StoredTestEventId("ParentID");
	private static final String DUMMY_NAME = "TestEvent";
	private static final String DUMMY_STREAM_NAME = "TestStream";
	private static final UUID DUMMY_UUID = UUID.randomUUID();
	private static final Instant DUMMY_START_TIMESTAMP = Instant.now();

	private TestEventToStoreBuilder eventBuilder;

	private int messageSequence = 0;

	@BeforeClass
	public void prepare() throws CradleStorageException
	{
		eventBuilder = new TestEventToStoreBuilder();
	}

	private Collection<StoredMessageId> generateMessageIds()
	{
		Collection<StoredMessageId> result = new ArrayList<>();
		for (int i = 0; i < 10; i++)
		{
			result.add(new StoredMessageId(DUMMY_STREAM_NAME, Direction.FIRST, ++messageSequence));
		}
		return result;
	}

	private StoredTestEventBatch generateBatch() throws CradleStorageException
	{
		TestEventBatchToStore batchToStore = new TestEventBatchToStore();
		batchToStore.setParentId(PARENT_ID);

		StoredTestEventBatch batch = new StoredTestEventBatch(batchToStore);
		for (int i = 0; i < 10; i++)
		{
			batch.addTestEvent(eventBuilder.id(new StoredTestEventId(UUID.randomUUID().toString()))
							.name(DUMMY_NAME)
							.startTimestamp(DUMMY_START_TIMESTAMP)
							.parentId(PARENT_ID)
							.messageIds(generateMessageIds())
							.content("Test content".getBytes())
							.build());
		}
		return batch;
	}

	@Test
	public void serializeDeserializeEventBatch() throws CradleStorageException, IOException
	{
		StoredTestEventBatch expected = generateBatch();
		TestEventEntity entity = new TestEventEntity(expected, DUMMY_UUID);
		StoredTestEventBatch actual = entity.toStoredTestEventBatch();
		assertThatObject(actual).usingRecursiveComparison().isEqualTo(expected);
		messageSequence = 0;
		for (BatchedStoredTestEvent event : actual.getTestEvents())
			assertThat(event.getMessageIds())
					.as("Check event %s linked ids", event.getId(), event.getMessageIds())
					.isEqualTo(generateMessageIds());
	}

}