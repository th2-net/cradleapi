/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.testevents;

import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.exactpro.cradle.utils.CradleIdException;
import com.exactpro.cradle.utils.CradleStorageException;

public class StoredTestEventTest
{
	private MinimalTestEventToStoreBuilder batchSettingsBuilder;
	private TestEventToStoreBuilder eventBuilder;
	private StoredTestEventId batchId;
	private MinimalTestEventToStore batchSettings;
	private StoredTestEventBatch batch;
	
	@BeforeClass
	public void prepare()
	{
		batchSettingsBuilder = MinimalTestEventToStore.newMinimalTestEventBuilder();
		eventBuilder = new TestEventToStoreBuilder();
		batchId = new StoredTestEventId(UUID.randomUUID().toString());
		batchSettings = batchSettingsBuilder
				.id(batchId)
				.build();
	}
	
	@BeforeMethod
	public void prepareBatch() throws CradleStorageException
	{
		batch = new StoredTestEventBatch(batchSettings);
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
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event ID cannot be null")
	public void batchIdMustBeSet() throws CradleStorageException
	{
		new StoredTestEventBatch(batchSettingsBuilder.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch is full")
	public void batchIsLimited() throws CradleIdException, CradleStorageException
	{
		for (int i = 0; i <= StoredTestEventBatch.MAX_EVENTS_NUMBER+1; i++)
			batch.addTestEvent(eventBuilder
					.id(new StoredTestEventId(Integer.toString(i)))
					.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Batch is full")
	public void batchContentIsLimited() throws CradleIdException, CradleStorageException
	{
		byte[] content = new byte[5000];
		for (int i = 0; i <= (StoredTestEventBatch.MAX_EVENTS_SIZE/content.length)+1; i++)
			batch.addTestEvent(eventBuilder
					.id(new StoredTestEventId(Integer.toString(i)))
					.content(content)
					.build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = "Test event with ID .* is already present in batch")
	public void duplicateIds() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId eventId = new StoredTestEventId("AAA");
		batch.addTestEvent(eventBuilder.id(eventId).build());
		batch.addTestEvent(eventBuilder.id(eventId).build());
	}
	
	@Test(expectedExceptions = {CradleStorageException.class}, expectedExceptionsMessageRegExp = ".* stored in this batch .*")
	public void externalReferences() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId parentId = new StoredTestEventId("1");
		batch.addTestEvent(eventBuilder.id(parentId).build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("2")).build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("3")).parentId(parentId).build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("4")).parentId(new StoredTestEventId("XXX")).build());
	}
	
	@Test
	public void referenceToBatch() throws CradleIdException, CradleStorageException
	{
		StoredTestEventId parentId = new StoredTestEventId("1");
		batch.addTestEvent(eventBuilder.id(parentId).build());
		//Events 2 and 3 are actually children of the batch: 
		//2 is root event, so it is presumed to be child of the batch
		//3 references the batch, so it is presumed to be root event
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("2")).build());
		batch.addTestEvent(eventBuilder.id(new StoredTestEventId("3")).parentId(batch.getId()).build());
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
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId("2"))
						.parentId(parentEvent.getId())
						.build());
		
		Assert.assertEquals(parentEvent.getChildren().contains(childEvent), true, "Children are aligned with their parent");
	}
	
	@Test
	public void rootIsRoot() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
				.id(new StoredTestEventId("1"))
				.parentId(null)
				.build());
		Assert.assertEquals(batch.getRootTestEvents().contains(parentEvent), true, "Root event is listed in roots");
	}
	
	@Test
	public void referenceToBatchIsRoot() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
				.id(new StoredTestEventId("1"))
				.parentId(batch.getId())
				.build());
		Assert.assertEquals(batch.getRootTestEvents().contains(parentEvent), true, "Event that referenced the batch is listed in roots");
	}
	
	@Test
	public void childIsNotRoot() throws CradleStorageException
	{
		BatchedStoredTestEvent parentEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId("1"))
						.build()),
				childEvent = batch.addTestEvent(eventBuilder
						.id(new StoredTestEventId("2"))
						.parentId(parentEvent.getId())
						.build());

		Assert.assertEquals(batch.getRootTestEvents().contains(childEvent), false, "Child event is not listed in roots");
	}
}
