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

public class CradleStorageException extends Exception
{
	private static final long serialVersionUID = 2492181993439645841L;

	public CradleStorageException(String message)
	{
		super(message);
	}
	
	public CradleStorageException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
