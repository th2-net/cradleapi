/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.utils;

import java.io.IOException;

import com.exactpro.cradle.testevents.StoredTestEvent;

public class TestEventException extends IOException
{
	private static final long serialVersionUID = 171332444673041240L;
	
	private final StoredTestEvent testEvent;
	
	public TestEventException(String message, Throwable cause, StoredTestEvent testEvent)
	{
		super(message, cause);
		this.testEvent = testEvent;
	}
	
	
	public StoredTestEvent getTestEvent()
	{
		return testEvent;
	}
}
