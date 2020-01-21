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

import com.exactpro.cradle.utils.CradleStorageException;

public interface ReportNavigator
{
	/**
	 * Searches report in storage according to passed parameters and sets it as current report
	 * @param reportId ID of stored report
	 * @return found report
	 * @throws CradleStorageException if storage doesn't contain report for given parameters
	 */
	StoredReport setCurrentReport(String reportId) throws CradleStorageException;

	/**
	 * @return current report the navigator points to
	 */
	StoredReport getCurrentReport();

	/**
	 * Switches to report next to current one and makes it current report
	 * @return next report
	 * @throws CradleStorageException if there is no next report
	 */
	StoredReport getNextReport() throws CradleStorageException;

	/**
	 * Switches to report previous to current one and makes it current report
	 * @return previous report
	 * @throws CradleStorageException if there is no previous report
	 */
	StoredReport getPrevReport() throws CradleStorageException;
}

