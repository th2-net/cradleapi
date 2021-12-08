/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import java.util.UUID;

import com.exactpro.cradle.serialization.EventsSizeCalculator;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;

public class StoredTestEventTest
{
	private static final StoredTestEventId DUMMY_ID = new StoredTestEventId("123");
	private static final String DUMMY_NAME = "TestEvent";
	private static final Instant DUMMY_START_TIMESTAMP = Instant.now();
	
	private TestEventBatchToStoreBuilder batchSettingsBuilder;
	private TestEventToStoreBuilder eventBuilder;
	private StoredTestEventId batchId;
	private TestEventBatchToStore batchSettings;
	private StoredTestEventBatch batch;
	
	@BeforeClass
	public void prepare()
	{
		batchSettingsBuilder = TestEventBatchToStore.builder();
		eventBuilder = new TestEventToStoreBuilder();
		batchId = new StoredTestEventId(UUID.randomUUID().toString());
		batchSettings = batchSettingsBuilder
				.id(batchId)
				.parentId(new StoredTestEventId("BatchID"))
				.build();
	}
	
	@BeforeMethod
	public void prepareBatch() throws CradleStorageException
	{
		batch = new StoredTestEventBatch(batchSettings);
	}
	
	@DataProvider(name = "invalid events")
	public Object[][] invalidEvents()
	{
		return new Object[][]
				{
					{eventBuilder.build()},  //Empty event
					{eventBuilder.id(DUMMY_ID).build()},
					{eventBuilder.name(DUMMY_NAME).build()},
					{eventBuilder.startTimestamp(DUMMY_START_TIMESTAMP).build()},
					{eventBuilder.id(DUMMY_ID).name(DUMMY_NAME).build()},
					{eventBuilder.id(DUMMY_ID).startTimestamp(DUMMY_START_TIMESTAMP).build()},
					{eventBuilder.name(DUMMY_NAME).startTimestamp(DUMMY_START_TIMESTAMP).build()}
				};
	}
	
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event ID cannot be null")
	public void eventIdMustBeSet() throws CradleStorageException
	{
		new StoredTestEventSingle(new TestEventToStoreBuilder().build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event cannot reference itself")
	public void selfReference() throws CradleStorageException
	{
		StoredTestEventId eventId = new StoredTestEventId("A");
		new StoredTestEventSingle(new TestEventToStoreBuilder().id(eventId).parentId(eventId).build());
	}
	
	@Test
	public void batchIdIsAutogenerated() throws CradleStorageException
	{
		StoredTestEventBatch batch = new StoredTestEventBatch(batchSettingsBuilder.parentId(new StoredTestEventId("zzz")).build());
		StoredTestEventId batchId = batch.getId();
		Assert.assertEquals(batchId != null && !batchId.toString().isEmpty(), true, "Batch ID is automatically generated as a non-empty value");
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch must have a parent")
	public void batchParentMustBeSet() throws CradleStorageException
	{
		new StoredTestEventBatch(batchSettingsBuilder.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Event .* must have a parent.*")
	public void parentMustBeSet() throws CradleStorageException
	{
		batch.addTestEvent(eventBuilder
				.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch has not enough space to hold given test event")
	public void batchContentIsLimited() throws CradleIdException, CradleStorageException
	{
		byte[] content = new byte[5000];
		for (int i = 0; i <= (StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE/content.length)+1; i++)
			batch.addTestEvent(eventBuilder
					.id(new StoredTestEventId(Integer.toString(i)))
					.name(DUMMY_NAME)
					.startTimestamp(DUMMY_START_TIMESTAMP)
					.parentId(batch.getParentId())
					.content(content)
					.build());
	}
	
	@Test
	public void batchCountsSpaceLeft() throws CradleStorageException
	{
		byte[] content = new byte[StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE/2];
		long left = batch.getSpaceLeft();

		TestEventToStore build = eventBuilder
				.id(new StoredTestEventId(Integer.toString(1)))
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(batch.getParentId())
				.content(content)
				.build();

		batch.addTestEvent(build);
		
		Assert.assertEquals(batch.getSpaceLeft(), left - EventsSizeCalculator.calculateRecordSizeInBatch(build), "Batch counts space left");
	}
	
	@Test
	public void batchChecksSpaceLeft() throws CradleStorageException
	{
		byte[] content = new byte[StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE / 2];
		
		TestEventToStore event = eventBuilder
				.id(new StoredTestEventId(Integer.toString(1)))
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(batch.getParentId())
				.content(content)
				.build();
		batch.addTestEvent(event);
		Assert.assertEquals(batch.hasSpace(event), false, "Batch shows if it has space to hold given test event");
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event with ID .* is already present in batch")
	public void duplicateIds() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId eventId = new StoredTestEventId("AAA");
		batch.addTestEvent(eventBuilder.id(eventId).name(DUMMY_NAME).startTimestamp(DUMMY_START_TIMESTAMP).parentId(batch.getParentId()).build());
		batch.addTestEvent(eventBuilder.id(eventId).name(DUMMY_NAME).startTimestamp(DUMMY_START_TIMESTAMP).parentId(batch.getParentId()).build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = ".* 'XXX' .* stored in this batch .*")
	public void externalReferences() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId parentId = new StoredTestEventId("1");
		batch.addTestEvent(eventBuilder.id(parentId)
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(batch.getParentId())
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("2"))
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(batch.getParentId())
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("3"))
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(parentId)
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("4"))
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(new StoredTestEventId("XXX"))
				.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = ".* stored in this batch .*")
	public void referenceToBatch() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId parentId = new StoredTestEventId("1");
		batch.addTestEvent(eventBuilder.id(parentId)
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(batch.getParentId())
				.build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("2"))
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(batch.getId())
				.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event cannot reference itself")
	public void selfReferenceInBatch() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId eventId = new StoredTestEventId("1");
		batch.addTestEvent(eventBuilder.id(eventId).parentId(eventId).build());
	}
	
	@Test
	public void childrenAligned() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId("1"))
						.name(DUMMY_NAME)
						.startTimestamp(DUMMY_START_TIMESTAMP)
						.parentId(batch.getParentId())
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId("2"))
						.name(DUMMY_NAME)
						.startTimestamp(DUMMY_START_TIMESTAMP)
						.parentId(parentEvent.getId())
						.build());
		
		Assert.assertEquals(parentEvent.getChildren().contains(childEvent), true, "Children are aligned with their parent");
	}
	
	@Test
	public void rootIsRoot() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
				.id(DUMMY_ID)
				.name(DUMMY_NAME)
				.startTimestamp(DUMMY_START_TIMESTAMP)
				.parentId(batch.getParentId())
				.build());
		Assert.assertEquals(batch.getRootTestEvents().contains(parentEvent), true, "Root event is listed in roots");
	}
	
	@Test
	public void childIsNotRoot() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId("1"))
						.name(DUMMY_NAME)
						.startTimestamp(DUMMY_START_TIMESTAMP)
						.parentId(batch.getParentId())
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId("2"))
						.name(DUMMY_NAME)
						.startTimestamp(DUMMY_START_TIMESTAMP)
						.parentId(parentEvent.getId())
						.build());

		Assert.assertEquals(batch.getRootTestEvents().contains(childEvent), false, "Child event is not listed in roots");
	}
	
	@Test(dataProvider = "invalid events",
			expectedExceptions = {CradleStorageException.class})
	public void eventValidation(TestEventToStore event) throws CradleStorageException
	{
		batch.addTestEvent(event);
	}
}
