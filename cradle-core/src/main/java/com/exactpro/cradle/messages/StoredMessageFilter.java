/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.messages;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import com.exactpro.cradle.Direction;

public class StoredMessageFilter
{
	public enum DirectionFilter {
		Any(null),
		Sent(Direction.SENT),
		Received(Direction.RECEIVED);

		final Direction value;

		DirectionFilter(Direction value)
		{
			this.value = value;
		}

		public Direction getValue()
		{
			return value;
		}

		public boolean checkDirection(Direction direction) {
			return value == null || value == direction;
		}
	}

	protected Instant periodFrom;
	protected Instant periodTo;
	protected Set<String> streams;
	protected DirectionFilter directionFilter;

	public StoredMessageFilter() {
		directionFilter = DirectionFilter.Any;
	}

	public boolean isEmpty() {
		return periodFrom == null && periodTo == null && directionFilter == null && (streams == null || streams.isEmpty());
	}

	public void copy(StoredMessageFilter otherFilter)
	{
		periodFrom = otherFilter.periodFrom;
		periodTo = otherFilter.periodTo;
		streams = otherFilter.streams != null ? new HashSet<>(otherFilter.streams) : null;
		directionFilter = otherFilter.directionFilter;
	}

	public Instant getPeriodFrom()
	{
		return periodFrom;
	}

	public void setPeriodFrom(Instant periodFrom)
	{
		this.periodFrom = periodFrom;
	}

	public Instant getPeriodTo()
	{
		return periodTo;
	}

	public void setPeriodTo(Instant periodTo)
	{
		this.periodTo = periodTo;
	}

	public Set<String> getStreams()
	{
		return streams;
	}

	public void setStreams(Set<String> streams)
	{
		this.streams = streams;
	}

	public DirectionFilter getDirectionFilter()
	{
		return directionFilter;
	}

	public void setDirectionFilter(DirectionFilter directionFilter)
	{
		this.directionFilter = directionFilter;
	}

	/**
	 * Returns true if message passes the filter
	 *
	 * @param message Message to check
	 * @return true, if all this filter's conditions are satisfied
	 */
	public boolean checkMessage(StoredMessage message)
	{
		if (directionFilter != null && !directionFilter.checkDirection(message.getDirection()))
			return false;

		if (periodFrom != null && message.getTimestamp().isBefore(periodFrom))
			return false;

		if (periodTo != null && message.getTimestamp().isAfter(periodTo))
			return false;

		if (streams != null && !streams.isEmpty() && !streams.contains(message.getStreamName()))
			return false;

		return true;
	}

	public void clear()
	{
		periodFrom = null;
		periodTo = null;
		streams = null;
		directionFilter = DirectionFilter.Any;
	}
}
