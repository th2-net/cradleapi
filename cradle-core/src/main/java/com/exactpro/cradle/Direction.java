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

public enum Direction
{
	FIRST("first"),
	SECOND("second");
	
	private final String label;

	private Direction(String label)
	{
		this.label = label;
	}
	
	public String getLabel()
	{
		return label;
	}
	
	public static Direction byLabel(String label)
	{
		for (Direction d : values())
		{
			if (d.getLabel().equals(label))
				return d;
		}
		return null;
	}
}
