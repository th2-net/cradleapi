/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle;

/**
 * Default implementation of CradleManager that does nothing and works like a stub.
 */
public class DefaultCradleManager extends CradleManager
{
	@Override
	protected CradleStorage createStorage()
	{
		return null;
	}
	
	@Override
	public MessageNavigator getStorageMessageNavigator()
	{
		return null;
	}

	@Override
	public ReportNavigator getStorageReportNavigator()
	{
		return null;
	}
}
