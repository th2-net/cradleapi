/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.ReportNavigator;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.utils.CradleStorageException;

public class CassandraReportNavigator implements ReportNavigator
{
	private final CassandraCradleStorage storage;
	private StoredReport currentReport;
	private String currentReportId;

	public CassandraReportNavigator(CassandraCradleStorage storage)
	{
		this.storage = storage;
	}

	@Override
	public StoredReport getCurrentReport()
	{
		return currentReport;
	}

	@Override
	public StoredReport setCurrentReport(String reportId) throws CradleStorageException
	{
		StoredReport report;
		if (!StringUtils.equals(reportId, currentReportId))
		{
			try
			{
				report = storage.getReport(reportId);
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Error while getting report from storage for record ID '"+reportId+ "'", e);
			}
		}
		else
			report = currentReport;

		if (report == null)
			throw new CradleStorageException("Record ID is invalid or does not point to a stored report");

		currentReportId = reportId;
		currentReport = report;

		return currentReport;
	}


	@Override
	public StoredReport getNextReport() throws CradleStorageException
	{
		if (currentReportId == null) // Current report wasn't set, so we start from the very first report in storage
		{
			UUID firstReportId = storage.getExtremeRecordId(CassandraCradleStorage.ExtremeFunction.MIN,
					storage.getSettings().getReportsTableName());
			if (firstReportId == null) // There are no stored reports now
				return null;

			return setCurrentReport(firstReportId.toString());
		}
		else
		{
			String newId = storage.getNextReportId(currentReportId);
			if (newId == null)
				throw new CradleStorageException("Action not available: this is the last stored report");

			StoredReport report;
			try
			{
				report = storage.getReport(newId);
			}
			catch (Exception e)
			{
				throw new CradleStorageException("Error while getting report from storage", e);
			}

			if (report == null)
				throw new CradleStorageException("Next record is not available, record ID '"+newId+"'");

			currentReportId = newId;
			currentReport = report;
		}

		return currentReport;
	}

	@Override
	public StoredReport getPrevReport() throws CradleStorageException
	{
		String newId = storage.getPrevReportId(currentReportId);
		if (newId == null)
			throw new CradleStorageException("Action not available: this is the first stored report");

		StoredReport report;
		try
		{
			report = storage.getReport(newId);
		}
		catch (Exception e)
		{
			throw new CradleStorageException("Error while getting report from storage", e);
		}

		if (report == null)
			throw new CradleStorageException("Previous record is not available, record ID '"+newId+"'");

		currentReportId = newId;
		currentReport = report;

		return currentReport;
	}
}
