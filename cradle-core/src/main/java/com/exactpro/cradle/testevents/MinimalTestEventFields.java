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

/**
 * Interface to access minimal meta-data fields set of test event
 */
public interface MinimalTestEventFields
{
	StoredTestEventId getId();
	String getName();
	String getType();
	StoredTestEventId getParentId();
}
