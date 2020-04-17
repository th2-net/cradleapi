/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.utils;

public class CradleIdException extends Exception
{
	private static final long serialVersionUID = 5461840136011810462L;

	public CradleIdException(String message)
	{
		super(message);
	}
	
	public CradleIdException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
