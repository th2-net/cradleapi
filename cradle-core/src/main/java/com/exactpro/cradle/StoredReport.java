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
import java.util.UUID;

public class StoredReport
{
	private UUID id;
	private Instant timestamp;
	private String reportPath;
	private boolean compressed;
	private byte[] reportContent;
	private String matrixName;

	public StoredReport()
	{
	}

	public StoredReport(UUID id, Instant timestamp, String reportPath, boolean compressed, byte[] reportContent,
	                    String matrixName)
	{
		this.id = id;
		this.timestamp = timestamp;
		this.reportPath = reportPath;
		this.compressed = compressed;
		this.reportContent = reportContent;
		this.matrixName = matrixName;
	}

	public UUID getId()
	{
		return id;
	}

	public Instant getTimestamp()
	{
		return timestamp;
	}

	public String getReportPath()
	{
		return reportPath;
	}

	public boolean isCompressed()
	{
		return compressed;
	}

	public byte[] getReportContent()
	{
		return reportContent;
	}

	public String getMatrixName()
	{
		return matrixName;
	}
}
