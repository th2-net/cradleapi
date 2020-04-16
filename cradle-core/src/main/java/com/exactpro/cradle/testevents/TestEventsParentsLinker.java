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

import java.io.IOException;
import java.util.List;

public interface TestEventsParentsLinker
{
	/**
	 * Retrieves IDs of children test events for given parent test event ID
	 * @param parentId ID of parent test event
	 * @return list of children test event IDs
	 * @throws IOException if test event data retrieval failed
	 */
	List<StoredTestEventId> getChildrenIds(StoredTestEventId parentId) throws IOException;

	/**
	 * Checks if test event has children test events
	 * @param parentId ID of parent test event
	 * @return true if test event has children test events, false otherwise
	 * @throws IOException if test event data retrieval failed
	 */
	boolean isTestEventHasChildren(StoredTestEventId parentId) throws IOException;
}
