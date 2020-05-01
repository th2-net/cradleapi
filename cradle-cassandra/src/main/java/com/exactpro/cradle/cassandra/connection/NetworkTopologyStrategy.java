/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NetworkTopologyStrategy
{
	private final Map<String, Integer> strategy;
	
	public NetworkTopologyStrategy(Map<String, Integer> strategy)
	{
		this.strategy = new HashMap<>(strategy);
	}
	
	public Map<String, Integer> asMap()
	{
		return Collections.unmodifiableMap(strategy);
	}
}
