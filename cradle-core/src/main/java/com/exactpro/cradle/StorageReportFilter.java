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

import java.time.Instant;

public class StorageReportFilter
{
	protected Instant periodFrom;
	protected Instant periodTo;

	public void copy(StorageMessageFilter otherFilter)
	{
		periodFrom = otherFilter.periodFrom;
		periodTo = otherFilter.periodTo;
	}

	public boolean isEmpty() {
		return periodFrom == null && periodTo == null;
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

	/**
	 * Returns true if report passes the filter
	 *
	 * @param report Report to check
	 * @return true, if all this filter's conditions are satisfied
	 */
	public boolean checkReport(StoredReport report)
	{
		if (periodFrom != null && report.getTimestamp().isBefore(periodFrom))
			return false;

		if (periodTo != null && report.getTimestamp().isAfter(periodTo))
			return false;

		return true;
	}

	public void clear()
	{
		periodFrom = null;
		periodTo = null;
	}
}
