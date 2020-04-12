/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.reports;

import java.io.Serializable;

/**
 * Holds ID of a report stored in Cradle
 */
public class StoredReportId implements Serializable
{
	private static final long serialVersionUID = 5273290999519261075L;
	
	private final String id;
	
	public StoredReportId(String id)
	{
		this.id = id;
	}
	
	
	public String getId()
	{
		return id;
	}
	
	
	@Override
	public String toString()
	{
		return id;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StoredReportId other = (StoredReportId) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
}