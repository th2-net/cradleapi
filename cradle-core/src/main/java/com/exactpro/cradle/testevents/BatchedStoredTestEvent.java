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

import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;

/**
 * Holds information about one test event stored in batch of events ({@link StoredTestEventBatch})
 */
public class BatchedStoredTestEvent implements StoredTestEventWithContent, Serializable
{
	private static final long serialVersionUID = 1052669983594471856L;
	
	private final StoredTestEventId id;
	private final String name,
			type;
	private final StoredTestEventId parentId;
	private final Instant startTimestamp,
	endTimestamp;
	private final boolean success;
	private final byte[] content;
	
	private final transient StoredTestEventBatch batch;
	
	public BatchedStoredTestEvent(StoredTestEventWithContent event, StoredTestEventBatch batch)
	{
		this.id = event.getId();
		this.name = event.getName();
		this.type = event.getType();
		this.parentId = event.getParentId();
		this.startTimestamp = event.getStartTimestamp();
		this.endTimestamp = event.getEndTimestamp();
		this.success = event.isSuccess();
		this.content = event.getContent();
		
		this.batch = batch;
	}
	
	
	@Override
	public StoredTestEventId getId()
	{
		return id;
	}
	
	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public String getType()
	{
		return type;
	}
	
	@Override
	public StoredTestEventId getParentId()
	{
		return parentId;
	}
	
	@Override
	public Instant getStartTimestamp()
	{
		return startTimestamp;
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
	public byte[] getContent()
	{
		return content;
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
